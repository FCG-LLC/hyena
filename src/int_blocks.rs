use bincode::{serialize, deserialize, Infinite};

use api::ScanComparison;
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
    Int32Sparse(Int32SparseBlock),
    Int16Sparse(Int16SparseBlock),
    Int8Sparse(Int8SparseBlock),
    StringBlock(StringBlock)
}

impl Block {
    pub fn create_block(block_type: &BlockType) -> Block {
        match block_type {
            &BlockType::Int64Dense => Block::Int64Dense(Int64DenseBlock { data: Vec::new() }),
            &BlockType::Int64Sparse => Block::Int64Sparse(Int64SparseBlock { data: Vec::new() }),
            &BlockType::Int32Sparse => Block::Int32Sparse(Int32SparseBlock { data: Vec::new() }),
            &BlockType::Int16Sparse => Block::Int16Sparse(Int16SparseBlock { data: Vec::new() }),
            &BlockType::Int8Sparse => Block::Int8Sparse(Int8SparseBlock { data: Vec::new() }),
            &BlockType::String => Block::StringBlock(StringBlock::new())
        }
    }

    pub fn len(&self) -> usize {
        match self {
            &Block::Int64Dense(ref b) => b.data.len(),
            &Block::Int64Sparse(ref b) => b.data.len(),
            &Block::Int32Sparse(ref b) => b.data.len(),
            &Block::Int16Sparse(ref b) => b.data.len(),
            &Block::Int8Sparse(ref b) => b.data.len(),
            &Block::StringBlock(ref b) => b.index_data.len()
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
            &Block::Int16Sparse(ref b) => {
                output_block = Block::Int16Sparse(b.filter_scan_results(scan_consumer));
            },
            &Block::Int8Sparse(ref b) => {
                output_block = Block::Int8Sparse(b.filter_scan_results(scan_consumer));
            },
            &Block::StringBlock(ref b) => {
                output_block = Block::StringBlock(b.filter_scan_results(scan_consumer))
            },
            _ => panic!("Unrecognized block type")
        }

        output_block
    }
}

impl Scannable<String> for Block {
    fn scan(&self, op: ScanComparison, val: &String, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::StringBlock(ref b) => b.scan(op, val, scan_consumer),
            _ => panic!("Wrong block type for String scan")
        }
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

impl Scannable<u16> for Block {
    fn scan(&self, op: ScanComparison, val: &u16, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::Int16Sparse(ref b) => b.scan(op, val, scan_consumer),
            _ => println!("Unrecognized u32 block type")
        }
    }
}

impl Scannable<u8> for Block {
    fn scan(&self, op: ScanComparison, val: &u8, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::Int8Sparse(ref b) => b.scan(op, val, scan_consumer),
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

// As of now this is byte array essentially
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StringBlock {
    // Pair: offset, start position in array; the end position might be implied
    pub index_data : Vec<(u32, usize)>,
    pub str_data : Vec<u8>
}

impl StringBlock {
    pub fn new() -> StringBlock {
        StringBlock{ index_data: Vec::new(), str_data: Vec::new() }
    }

    pub fn append(&mut self, o: u32, v: &[u8]) {
        let last_index = self.str_data.len();
        let str_bytes = v;
        self.index_data.push((o, last_index));
        self.str_data.extend_from_slice(str_bytes);
    }

    pub fn filter_scan_results(&self, scan_consumer: &BlockScanConsumer) -> StringBlock {
        let mut out_block = StringBlock::new();
        // TODO: binary search-like operations would be faster usually (binary-search + scans)

        let mut block_data_index = 0 as usize;
        let mut scan_data_index = 0 as usize;

        while scan_data_index < scan_consumer.matching_offsets.len() && block_data_index < self.index_data.len() {
            let target_offset = scan_consumer.matching_offsets[scan_data_index];
            while block_data_index < self.index_data.len() && self.index_data[block_data_index].0 < target_offset {
                block_data_index += 1;
            }

            if block_data_index < self.index_data.len() && self.index_data[block_data_index].0 == target_offset {
                let arr_start_position = self.index_data[block_data_index].1.to_owned();
                let arr_end_position = if block_data_index < self.index_data.len()-1 {
                    self.index_data[block_data_index+1].1.to_owned()
                } else {
                    self.str_data.len().to_owned()
                };

                let val = &self.str_data[arr_start_position..arr_end_position];
                out_block.append(scan_data_index as u32, val);
                block_data_index += 1;
            }

            // Move on regardless
           scan_data_index += 1;
        }

        out_block
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

        while offsets_index < scan_consumer.matching_offsets.len() && data_index < self.data.len() {
            let target_offset = scan_consumer.matching_offsets[offsets_index];
            while data_index < self.data.len() && self.data[data_index].0 < target_offset {
                data_index += 1;
            }

            if data_index < self.data.len() && self.data[data_index].0 == target_offset {
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
pub type Int16SparseBlock = TSparseBlock<u16>;
pub type Int8SparseBlock = TSparseBlock<u8>;


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

impl Int16SparseBlock {
    pub fn new() -> Int16SparseBlock {
        Int16SparseBlock { data: Vec::new() }
    }
    pub fn encapsulate_in_block(self) -> Block {
        Block::Int16Sparse(self)
    }
}

impl Int8SparseBlock {
    pub fn new() -> Int8SparseBlock {
        Int8SparseBlock { data: Vec::new() }
    }
    pub fn encapsulate_in_block(self) -> Block {
        Block::Int8Sparse(self)
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

impl Scannable<String> for StringBlock {
    /// Screams naiive!
    fn scan(&self, op : ScanComparison, str_val : &String, scan_consumer : &mut BlockScanConsumer) {
        let mut prev_offset = 0 as u32;
        let mut prev_position = 0 as usize;
        let val = str_val.as_bytes();
        let mut index = 0;

        for &(offset_usize, position) in self.index_data.iter() {
            let size = position - prev_position;
            let offset = offset_usize as u32;

            if index > 0 {
                match op {
                    ScanComparison::Eq => if size == val.len() && val == &self.str_data[prev_position..position] { scan_consumer.matching_offsets.push(prev_offset) },
                    ScanComparison::NotEq => {
                        if size != val.len() || val != &self.str_data[prev_position..position] { scan_consumer.matching_offsets.push(prev_offset) }
                    },
                    _ => println!("Ooops, this is string block - only == and <> are supported now")
                }
            }

            prev_position = position;
            prev_offset = offset;
            index += 1;
        }

        // last element
        // TODO: extract/refactor

        if index > 0 {
            let size = self.str_data.len() - prev_position;
            let offset = prev_offset;
            let position = self.str_data.len();

            match op {
                ScanComparison::Eq => if size == val.len() && val == &self.str_data[prev_position..position] { scan_consumer.matching_offsets.push(prev_offset) },
                ScanComparison::NotEq => if size != val.len() || val != &self.str_data[prev_position..position] { scan_consumer.matching_offsets.push(prev_offset) },
                _ => println!("Ooops, this is string block - only == and <> are supported now")
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

impl Scannable<u16> for Int16SparseBlock {
    fn scan(&self, op : ScanComparison, val : &u16, scan_consumer : &mut BlockScanConsumer) {
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

impl Scannable<u8> for Int8SparseBlock {
    fn scan(&self, op : ScanComparison, val : &u8, scan_consumer : &mut BlockScanConsumer) {
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
fn string_block() {
    let mut expected_block = StringBlock {
        index_data: vec![
            (0, 0), // foo
            (1, 3), // bar
            (2, 6), // ""
            (3, 6), // snafu
        ],
        str_data: "foobarsnafu".as_bytes().to_vec()
    };

    let mut str_block = StringBlock::new();
    str_block.append(0, "foo".as_bytes());
    str_block.append(1, "bar".as_bytes());
    str_block.append(2, "".as_bytes());
    str_block.append(3, "snafu".as_bytes());

    assert_eq!(expected_block, str_block);

    let mut consumer = BlockScanConsumer::new();
    str_block.scan(ScanComparison::Eq, &String::from("bar"), &mut consumer);
    assert_eq!(consumer.matching_offsets, vec![1]);

    consumer = BlockScanConsumer::new();
    str_block.scan(ScanComparison::NotEq, &String::from("bar"), &mut consumer);
    assert_eq!(consumer.matching_offsets, vec![0,2,3]);

    consumer = BlockScanConsumer::new();
    str_block.scan(ScanComparison::Eq, &String::from("snafu"), &mut consumer);
    assert_eq!(consumer.matching_offsets, vec![3]);
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
