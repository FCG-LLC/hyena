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

//    pub fn create_block_with_data<T>(block_type: &BlockType, initial_data: Vec<T>) -> Block {
//        match block_type {
//            &BlockType::Int64Dense => Block::Int64Dense(Int64DenseBlock { data: initial_data }),
//            &BlockType::Int64Sparse => Block::Int64Sparse(Int64SparseBlock { data: initial_data }),
//            &BlockType::Int32Sparse => Block::Int32Sparse(Int32SparseBlock { data: initial_data }),
//            _ => panic!("Not supported"),
//        }
//    }

    pub fn len(&self) -> usize {
        match self {
            &Block::Int64Dense(ref b) => b.data.len(),
            &Block::Int64Sparse(ref b) => b.data.len(),
            &Block::Int32Sparse(ref b) => b.data.len()
        }
    }

    fn consume(&self, scan_consumer : &BlockScanConsumer) -> Block {
        let output_block:Block;
        match self {
            &Block::Int64Dense(ref b) => {
                let data : Vec<u64> = Vec::new();
                for index in scan_consumer.matching_offsets {
                    data.push(b.data[index as usize]);
                }
                output_block = Block::create_block_with_data(BlockType::Int64Dense, data);
            },
            &Block::Int64Sparse(ref b) => {
                let data : Vec<(u32, u64)> = Vec::new();
                // TODO: binary search-like operations would be faster usually (binary-search + scans)
//                for (offset, value) in scan_consumer.matching_offsets {
//
//                }
                output_block = Block::create_block_with_data(BlockType::Int64Sparse, data);

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
    pub fn append(&mut self, v: u64) {
        self.data.push(v);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Int64SparseBlock {
    pub data : Vec<(u32,u64)>
}

impl Int64SparseBlock {
    pub fn append(&mut self, o: u32, v: u64) {
        self.data.push((o, v));
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Int32SparseBlock {
    pub data : Vec<(u32,u32)>
}

impl Int32SparseBlock {
    pub fn append(&mut self, o: u32, v: u32) {
        self.data.push((o, v));
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