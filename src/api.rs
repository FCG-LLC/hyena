use bincode::{serialize, deserialize, Infinite};
use catalog::{BlockType, Column, PartitionInfo};
use manager::{Manager, BlockCache};
use int_blocks::{Block, Int32SparseBlock, Int64DenseBlock, Int64SparseBlock, Scannable};
use std::time::Instant;
use scan::{BlockScanConsumer};

#[derive(Serialize, Deserialize, PartialEq)]
pub struct InsertMessage {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
    pub blocks : Vec<Block> // This can be done right now only because blocks are so trivial
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct ScanResultMessage {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
    pub blocks : Vec<Block> // This can be done right now only because blocks are so trivial
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanFilter {
    pub column : u32,
    pub op : ScanComparison,
    pub val : u64,
    pub str_val : Vec<u8>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts : u64,
    pub max_ts : u64,
    pub partition_id : u64,
    pub projection : Vec<u32>,
    pub filters : Vec<ScanFilter>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RefreshCatalogResponse {
    pub columns: Vec<Column>,
    pub available_partitions: Vec<PartitionInfo>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: BlockType
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GenericResponse {
    pub status : u32
}

impl GenericResponse {
    pub fn create_as_buf(status : u32) -> Vec<u8> {
        let resp = GenericResponse { status: status };
        serialize(&resp, Infinite).unwrap()
    }
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ApiOperation {
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ApiMessage {
    pub op_type : ApiOperation,
    pub payload : Vec<u8>
}

impl ApiMessage {
    pub fn extract_scan_request(&self) -> ScanRequest {
        assert_eq!(self.op_type, ApiOperation::Scan);

        let scan_request = deserialize(&self.payload[..]).unwrap();
        scan_request
    }

    pub fn extract_insert_message(&self) -> InsertMessage {
        assert_eq!(self.op_type, ApiOperation::Insert);

        let insert_message = deserialize(&self.payload[..]).unwrap();
        insert_message
    }

    pub fn extract_add_column_message(&self) -> AddColumnRequest {
        assert_eq!(self.op_type, ApiOperation::AddColumn);

        let column_message = deserialize(&self.payload[..]).unwrap();
        column_message
    }
}

impl ScanResultMessage {
    pub fn new() -> ScanResultMessage {
        ScanResultMessage {
            row_count: 0,
            col_count: 0,
            col_types: Vec::new(),
            blocks: Vec::new()
        }
    }
}

impl RefreshCatalogResponse {
    pub fn new(manager: &Manager) -> RefreshCatalogResponse {
        RefreshCatalogResponse {
            columns: manager.catalog.columns.to_owned(),
            available_partitions: manager.catalog.available_partitions.to_owned()
        }
    }
}

// FIXME: this is ugly copypasta

fn consume_empty_filter<'a>(manager : &Manager, cache : &'a mut BlockCache, consumer : &mut BlockScanConsumer) {
    let scanned_block = manager.load_block(&cache.partition_info, 0); // ts

    match &scanned_block {
        &Block::Int64Dense(ref x) => {
            for i in 0..x.data.len() {
                consumer.matching_offsets.push(i as u32);
            }
        },
        _ => println!("This is unexpected - TS is not here")
    }

    cache.cache_block(scanned_block, 0);
}

fn consume_filters<'a>(manager : &'a Manager, cache: &'a mut BlockCache, filter: &'a ScanFilter, mut consumer: &mut BlockScanConsumer) {
    let scanned_block = manager.load_block(&cache.partition_info, filter.column); // ts
    scanned_block.scan(filter.op.clone(), &filter.val, &mut consumer);
    cache.cache_block(scanned_block, filter.column);

    // FIXME: why following doesn't work and we need to use the above way?

//    let block_maybe = cache.cached_block_maybe(filter.column);
//    match block_maybe {
//        None => {
//            let scanned_block = manager.load_block(&cache.partition_info, filter.column); // ts
//            scanned_block.scan(filter.op.clone(), &filter.val, &mut consumer);
//            cache.cache_block(scanned_block, filter.column);
//        },
//        Some(ref x) => {
//            x.scan(filter.op.clone(), &filter.val, &mut consumer);
//        }
//    };

}

pub fn part_scan_and_materialize(manager: &Manager, req : &ScanRequest) -> ScanResultMessage {
    let scan_duration = Instant::now();

    let mut total_matched = 0;
    let mut total_materialized = 0;

    let mut scan_msg = ScanResultMessage::new();

    let part_info = &manager.find_partition_info(req.partition_id);

    let mut consumers:Vec<BlockScanConsumer> = Vec::new();
    let mut cache = BlockCache::new(part_info);

    if req.filters.is_empty() {
        let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
        consume_empty_filter(manager, &mut cache, &mut consumer);
        consumers.push(consumer);
    } else {
        for filter in &req.filters {
            let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
            consume_filters(manager, &mut cache, &filter, &mut consumer);
            consumers.push(consumer);
        }
    }

    let combined_consumer = BlockScanConsumer::merge_and_scans(&consumers);
    combined_consumer.materialize(&manager, &mut cache, &req.projection, &mut scan_msg);

    total_materialized += scan_msg.row_count;
    total_matched += combined_consumer.matching_offsets.len();

    println!("Scanning and matching/materializing {}/{} elements took {:?}", total_matched, total_materialized, scan_duration.elapsed());

    scan_msg
}

// FIXME: this is ugly copypasta

#[test]
fn inserting_works() {
    let mut test_msg:Vec<u8> = vec![];

    let base_ts = 1495490000 * 1000000;

    let insert_msg = InsertMessage {
        row_count: 3,
        col_count: 5,
        col_types: vec![(0, BlockType::Int64Dense), (1, BlockType::Int64Dense), (2, BlockType::Int64Sparse), (4, BlockType::Int64Sparse)],
        blocks: vec![
            Block::Int64Dense(Int64DenseBlock{
                data: vec![base_ts, base_ts+1000, base_ts+2000]
            }),
            Block::Int64Dense(Int64DenseBlock{
                data: vec![0, 0, 1, 2]
            }),
            Block::Int64Sparse(Int64SparseBlock{
                data: vec![(0, 100), (1, 200)]
            }),
            Block::Int64Sparse(Int64SparseBlock{
                data: vec![(2, 300), (3, 400)]
            }),
        ]
    };
    
    test_msg.extend(serialize(&insert_msg, Infinite).unwrap());

    println!("In test {:?}", test_msg);
//    insert_serialized_request(&test_msg);
}

#[test]
fn api_scan_message_serialization() {
    let scan_req = ScanRequest {
        min_ts: 100 as u64,
        max_ts: 200 as u64,
        partition_id: 0,
        filters: vec![
            ScanFilter {
                column: 5,
                op: ScanComparison::GtEq,
                val: 1000 as u64,
                str_val: vec![]
            }
        ],
        projection: vec![0,1,2,3]
    };

    let api_msg = ApiMessage {
        op_type: ApiOperation::Scan,
        payload: serialize(&scan_req, Infinite).unwrap()
    };

    let serialized_msg = serialize(&api_msg, Infinite).unwrap();

    println!("Filter #1: {:?}", serialize(&scan_req.filters[0], Infinite).unwrap());
    println!("Filters: {:?}", serialize(&scan_req.filters, Infinite).unwrap());
    println!("Projection: {:?}", serialize(&scan_req.projection, Infinite).unwrap());
    println!("Scan request: {:?}", serialize(&scan_req, Infinite).unwrap());
    println!("Payload length: {}", api_msg.payload.len());
    println!("Serialized api message for scan: {:?}", serialized_msg);
}

#[test]
fn api_refresh_catalog_serialization() {
    let pseudo_response = RefreshCatalogResponse{
        columns: vec![
            Column {
                data_type: BlockType::Int64Dense,
                name: String::from("ts")
            },
            Column {
                data_type: BlockType::Int32Sparse,
                name: String::from("source")
            }
        ],
        available_partitions: vec![
            PartitionInfo{
                min_ts: 100,
                max_ts: 200,
                id: 999,
                location: String::from("/foo/bar")
            }
        ]
    };

    let x:String = String::from("abc");
    println!("String response: {:?}", serialize(&x, Infinite).unwrap());
    println!("Pseudo catalog refresh response: {:?}", serialize(&pseudo_response, Infinite).unwrap());
}
