use bincode::{serialize, deserialize, Infinite};
use catalog::BlockType;
use manager::Manager;
use int_blocks::{Block, Int32SparseBlock, Int64DenseBlock, Int64SparseBlock};

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
    pub op : ScanComparison,
    pub val : u64
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts : u64,
    pub max_ts : u64,
    pub projection : Vec<u32>,
    pub filters : Vec<ScanFilter>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ApiOperation {
    Insert,
    Scan
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

pub fn insert_serialized_request(manager: &mut Manager, buf : &Vec<u8>) {
    let msg : InsertMessage = deserialize(&buf[..]).unwrap();

    manager.insert(&msg);
}

#[test]
fn it_works() {
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
fn api_message_serialization() {
    let scan_req = ScanRequest {
        min_ts: 100 as u64,
        max_ts: 200 as u64,
        filters: vec![
            ScanFilter {
                op: ScanComparison::GtEq,
                val: 1000 as u64
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