use bincode::{serialize, deserialize, Infinite};

use scan::ScanComparison;
use scan::BlockScanConsumer;
use catalog::BlockType;


// Sorry for this copypasta, it took me bit more time to make templates work and still had some issues, so consider this just a mock

pub trait Scannable<T> {
    fn scan(&self, op : ScanComparison, val : &T, scan_consumer : &mut BlockScanConsumer);
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Block {
    Int64Dense(Int64DenseBlock),
    Int64Sparse(Int64SparseBlock),
    Int32Sparse(Int32SparseBlock)
}

impl Block {
    pub fn create_block(block_type : &BlockType) -> Block {
        match block_type {
            &BlockType::Int64Dense => Block::Int64Dense(Int64DenseBlock{data : Vec::new()}),
            &BlockType::Int64Sparse => Block::Int64Sparse(Int64SparseBlock{data : Vec::new()}),
            &BlockType::Int32Sparse => Block::Int32Sparse(Int32SparseBlock{data : Vec::new()}),
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

    pub fn dump(&self, path:String) {
        let mut encoded: Vec<u8>;

        match self {
            &Block::Int64Dense(ref b) => encoded = serialize(b, Infinite).unwrap(),
            &Block::Int64Sparse(ref b) =>  encoded = serialize(b, Infinite).unwrap(),
            &Block::Int32Sparse(ref b) => encoded = serialize(b, Infinite).unwrap()
        }
    }

    pub fn load(&mut self, path:String) {
//        let arr:Vec<u8>;
//
//        match &mut self {
//            &Block::Int64Dense(ref mut b) => b.data = deserialize(&arr[..]).unwrap(),
//            &Block::Int64Sparse(ref mut b) => b.data = deserialize(&arr[..]).unwrap(),
//            &Block::Int32Sparse(ref mut b) => b.data = deserialize(&arr[..]).unwrap()
//        }
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
    fn scan(&self, op : ScanComparison, val : &u32, scan_consumer : &mut BlockScanConsumer) {
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