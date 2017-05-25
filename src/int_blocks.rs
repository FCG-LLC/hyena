use bincode::{serialize, deserialize, Infinite};

use scan::ScanComparison;
use scan::BlockScanConsumer;
use catalog::BlockType;


// Sorry for this copypasta, it took me bit more time to make templates work and still had some issues, so consider this just a mock

pub trait Scannable<T> {
    fn scan(&self, op : ScanComparison, val : &T, scan_consumer : &mut BlockScanConsumer);
//    fn consume(&self, scan_consumer : &BlockScanConsumer) -> Block;
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Block {
    Int64Dense(Int64DenseBlock),
    Int64Sparse(Int64SparseBlock),
    Int32Sparse(Int32SparseBlock)
}

impl Block {
    pub fn create_block(block_type: &BlockType) -> Block {
        match block_type {
            &BlockType::Int64Dense => Block::Int64Dense(Int64DenseBlock { data: Vec::new() }),
            &BlockType::Int64Sparse => Block::Int64Sparse(Int64SparseBlock { data: Vec::new() }),
            &BlockType::Int32Sparse => Block::Int32Sparse(Int32SparseBlock { data: Vec::new() }),
            _ => panic!("Not supported"),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            &Block::Int64Dense(ref b) => b.data.len(),
            &Block::Int64Sparse(ref b) => b.data.len(),
            &Block::Int32Sparse(ref b) => b.data.len()
        }
    }

    pub fn consume(&self, scan_consumer : &BlockScanConsumer) -> Block {
        let output_block:Block;

        match self {
            &Block::Int64Dense(ref b) => {
                let mut block = Int64DenseBlock::new();

                for index in &scan_consumer.matching_offsets {
                    block.data.push(b.data[*index as usize]);
                }
                output_block = Block::Int64Dense(block);
            },
            &Block::Int64Sparse(ref b) => {
                output_block = Block::Int64Sparse(b.filter_scan_results(scan_consumer));
            },
            &Block::Int32Sparse(ref b) => {
                output_block = Block::Int32Sparse(b.filter_scan_results(scan_consumer));
            },
            _ => panic!("Unrecognized u64 block type")
        }

        output_block
    }

}

impl Scannable<u64> for Block {
    fn scan(&self, op : ScanComparison, val : &u64, scan_consumer : &mut BlockScanConsumer) {
        match self {
            &Block::Int64Dense(ref b) => b.scan(op, val, scan_consumer),
            &Block::Int64Sparse(ref b) => b.scan(op, val, scan_consumer),
            _ => panic!("Unrecognized u64 block type")
        }
    }

}

impl Scannable<u32> for Block {
    fn scan(&self, op: ScanComparison, val: &u32, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::Int32Sparse(ref b) => b.scan(op, val, scan_consumer),
            _ => println!("Unrecognized u32 block type")
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Int64DenseBlock {
    pub data : Vec<u64>
}

impl Int64DenseBlock {
    pub fn new() -> Int64DenseBlock {
        Int64DenseBlock { data: Vec::new() }
    }

    pub fn append(&mut self, v: u64) {
        self.data.push(v);
    }

    pub fn encapsulate_in_block(self) -> Block {
        Block::Int64Dense(self)
    }

    pub fn filter_scan_results(&self, scan_consumer : &BlockScanConsumer) -> Int64DenseBlock {
        let mut out_block = Int64DenseBlock::new();

        for index in &scan_consumer.matching_offsets {
            out_block.data.push(self.data[*index as usize]);
        }

        return out_block;
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TSparseBlock<T:Clone> {
    pub data : Vec<(u32,T)>
}

impl<T : Clone> TSparseBlock<T> {
    pub fn append(&mut self, o: u32, v: T) {
        self.data.push((o, v));
    }

    pub fn filter_scan_results(&self, scan_consumer : &BlockScanConsumer) -> TSparseBlock<T> {
        let mut out_block = TSparseBlock { data: Vec::new() };

        // TODO: binary search-like operations would be faster usually (binary-search + scans)

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while offsets_index < scan_consumer.matching_offsets.len() {
            let target_offset = scan_consumer.matching_offsets[offsets_index];
            while self.data[data_index].0 < target_offset && data_index < self.data.len() {
                data_index += 1;
            }

            if self.data[data_index].0 == target_offset {
                let val:T = self.data[data_index].1.to_owned();
                out_block.append(offsets_index as u32, val);
                data_index += 1;
            }

            // Move on regardless
            offsets_index += 1;
        }

        out_block
    }
}

//#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub type Int64SparseBlock = TSparseBlock<u64>;
pub type Int32SparseBlock = TSparseBlock<u32>;


impl Int64SparseBlock {
    pub fn new() -> Int64SparseBlock {
        Int64SparseBlock { data: Vec::new() }
    }
    pub fn encapsulate_in_block(self) -> Block {
        Block::Int64Sparse(self)
    }
}

impl Int32SparseBlock {
    pub fn new() -> Int32SparseBlock {
        Int32SparseBlock { data: Vec::new() }
    }
    pub fn encapsulate_in_block(self) -> Block {
        Block::Int32Sparse(self)
    }
}

impl Scannable<u64> for Int64DenseBlock {
    fn scan(&self, op : ScanComparison, val : &u64, scan_consumer : &mut BlockScanConsumer) {
        for (offset_usize, value) in self.data.iter().enumerate() {
            let offset = offset_usize as u32;
            match op {
                ScanComparison::Lt => if value < val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::LtEq => if value <= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Eq => if value == val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::GtEq => if value >= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Gt => if value > val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::NotEq => if value != val { scan_consumer.matching_offsets.push(offset) },
            }
        }
    }
}


impl Scannable<u64> for Int64SparseBlock {
    fn scan(&self, op : ScanComparison, val : &u64, scan_consumer : &mut BlockScanConsumer) {
        for &(offset, value_ref) in self.data.iter() {
            let value = &value_ref;
            match op {
                ScanComparison::Lt => if value < val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::LtEq => if value <= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Eq => if value == val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::GtEq =>  if value >= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Gt => if value > val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::NotEq => if value != val { scan_consumer.matching_offsets.push(offset) },
            }
        }
    }
}

impl Scannable<u32> for Int32SparseBlock {
    fn scan(&self, op : ScanComparison, val : &u32, scan_consumer : &mut BlockScanConsumer) {
        for &(offset, value_ref) in self.data.iter() {
            let value = &value_ref;
            match op {
                ScanComparison::Lt => if value < val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::LtEq => if value <= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Eq => if value == val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::GtEq => if value >= val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::Gt => if value > val { scan_consumer.matching_offsets.push(offset) },
                ScanComparison::NotEq => if value != val { scan_consumer.matching_offsets.push(offset) },
            }
        }
    }
}



#[test]
fn it_filters_sparse_block() {
    let mut data_block = Int64SparseBlock {
        data: vec![
            (1, 100),
            (2, 200),
            (3, 300),
            (6, 600),
            (8, 800),
            (11, 1100)
        ]
    };

    let scan_consumer = BlockScanConsumer {
        matching_offsets: vec![2,3,4,11]
    };

    // The offsets are now changed to be with order of scan consumer

    let expected_output = Int64SparseBlock {
        data: vec![
            (0, 200),
            (1, 300),
            (3, 1100)
        ]
    };

    let actual_output = data_block.filter_scan_results(&scan_consumer);

    assert_eq!(expected_output, actual_output);
}
