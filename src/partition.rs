use bincode::{serialize, deserialize, Infinite};

use scan::ScanComparison;
use scan::BlockScanConsumer;
use int_blocks::Block;
use std::cmp;

// @jacek - we might assume that Partition will be be owned by a single thread always

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Partition {
    pub min_ts : u64,
    pub max_ts : u64,
    pub blocks : Vec<Block>
}

impl Partition {
    pub fn new() -> Partition {
        Partition { min_ts: 0, max_ts: 0, blocks: Vec::new() }
    }

    pub fn prepare(&mut self) {
        let ts_block = &self.blocks[0];
        self.min_ts = u64::max_value();

        match ts_block {
            &Block::Int64Dense(ref b) => for v in &b.data {
                self.min_ts = cmp::min(self.min_ts, *v);
                self.max_ts = cmp::max(self.max_ts, *v);
            },
            _ => panic!("No, timestamp/int64dense expected")
        }
    }

//    pub fn dump(&self, path:String) {
//        let encoded: Vec<u8> = serialize(&self.blocks, Infinite).unwrap();
//    }
}


