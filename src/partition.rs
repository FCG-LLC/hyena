use scan::BlockScanConsumer;
use api::ScanComparison;
use bincode::{serialize, deserialize, Infinite};
use int_blocks::Block;
use std::cmp;

// @jacek - we might assume that Partition will be be owned by a single thread always

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct PartitionMetadata {
    pub min_ts : u64,
    pub max_ts : u64,
    pub existing_blocks: Vec<u32>
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Partition {
    pub metadata : PartitionMetadata,
    pub blocks : Vec<Block>
}


impl Partition {
    pub fn new() -> Partition {
        Partition {
            metadata: PartitionMetadata {
                min_ts: 0,
                max_ts: 0,
                existing_blocks: Vec::new()
            },
            blocks: Vec::new()
        }
    }

    pub fn prepare(&mut self) {
        if self.blocks.len() == 0 { panic!("Seems this partition does not havy any blocks") };
        
        let ts_block = &self.blocks[0];
        self.metadata.min_ts = u64::max_value();

        match ts_block {
            &Block::Int64Dense(ref b) => for v in &b.data {
                self.metadata.min_ts = cmp::min(self.metadata.min_ts, *v);
                self.metadata.max_ts = cmp::max(self.metadata.max_ts, *v);
            },
            _ => panic!("No, timestamp/int64dense expected")
        }

        self.metadata.existing_blocks = Vec::new();
        
        for (i, block) in self.blocks.iter().enumerate() {
            if block.len() > 0 {
                self.metadata.existing_blocks.push(i as u32);
            }
        }
    }

//    pub fn dump(&self, path:String) {
//        let encoded: Vec<u8> = serialize(&self.blocks, Infinite).unwrap();
//    }
}


