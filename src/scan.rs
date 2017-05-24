use catalog::PartitionInfo;
use api::ScanResultMessage;
use catalog::Catalog;
use manager::Manager;

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

pub struct BlockScanConsumer {
    pub matching_offsets : Vec<u32>
}


impl BlockScanConsumer {
    pub fn merge_scans(&mut self, other_scans : &Vec<BlockScanConsumer>) {
        // TODO
    }
    
    pub fn materialize(&self, manager : &Manager, catalog : &Catalog, part_info : &PartitionInfo, projection : &Vec<u32>, msg : &mut ScanResultMessage) {
        // This should work only on empty message (different implementation is of course possible,
        // if you think it would make sense to merge results)
        assert_eq!(msg.row_count, 0);

        msg.row_count = self.matching_offsets.len() as u32;
        msg.col_count = projection.len() as u32;

        for col_index in projection {
//            let col_index = *col;
            let column = &catalog.columns[*col_index as usize];
            msg.col_types.push((*col_index, column.data_type.to_owned()));


            // Fetch block from disk
            let block = manager.load_block(part_info, *col_index);

        }
    }
}
