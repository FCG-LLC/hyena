use bincode::{serialize, deserialize, Infinite};

use api::ScanComparison;
use scan::BlockScanConsumer;
use catalog::BlockType;
use std::cmp;

// Sorry for this copypasta, it took me bit more time to make templates work and still had some issues, so consider this just a mock

pub trait Scannable<T> {
    fn scan(&self, op : ScanComparison, val : &T, scan_consumer : &mut BlockScanConsumer);
}

pub trait Deletable {
    fn delete(&mut self, offsets : &Vec<u32>);
}

pub trait Upsertable<T> {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &T);
    fn upsert(&mut self, data : &Block);
}

pub trait Movable {
    fn move_data(&mut self, target : &mut Block, scan_consumer : &BlockScanConsumer);
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

impl Deletable for Block {
    fn delete(&mut self, offsets : &Vec<u32>) {
        match self {
            &mut Block::StringBlock(ref mut b) => b.delete(offsets),
            &mut Block::Int64Sparse(ref mut b) => b.delete(offsets),
            &mut Block::Int32Sparse(ref mut b) => b.delete(offsets),
            &mut Block::Int16Sparse(ref mut b) => b.delete(offsets),
            &mut Block::Int8Sparse(ref mut b) => b.delete(offsets),
            _ => panic!("I don't know how to handle such block type")
        }
    }
}

impl Movable for Block {
    fn move_data(&mut self, target: &mut Block, scan_consumer: &BlockScanConsumer) {
        match self {
            &mut Block::StringBlock(ref mut b) => match target {
                &mut Block::StringBlock(ref mut c) => b.move_data(c, scan_consumer),
                _ => panic!("Not matching block types")
            },
            &mut Block::Int64Sparse(ref mut b) => match target {
                &mut Block::Int64Sparse(ref mut c) => b.move_data(c, scan_consumer),
                _ => panic!("Not matching block types")
            },
            &mut Block::Int32Sparse(ref mut b) => match target {
                &mut Block::Int32Sparse(ref mut c) => b.move_data(c, scan_consumer),
                _ => panic!("Not matching block types")
            },
            &mut Block::Int16Sparse(ref mut b) => match target {
                &mut Block::Int16Sparse(ref mut c) => b.move_data(c, scan_consumer),
                _ => panic!("Not matching block types")
            },
            &mut Block::Int8Sparse(ref mut b) => match target {
                &mut Block::Int8Sparse(ref mut c) => b.move_data(c, scan_consumer),
                _ => panic!("Not matching block types")
            },
            _ => panic!("I don't know how to handle such block type")
        }
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

impl Upsertable<String> for Block {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &String) {
        match self {
            &mut Block::StringBlock(ref mut b) => b.multi_upsert(offsets, val.as_bytes()),
            _ => panic!("Wrong block type for String scan")
        }
    }

    fn upsert(&mut self, data : &Block) {
        match self {
            &mut Block::StringBlock(ref mut b) => match data {
                &Block::StringBlock(ref c) => b.upsert(c),
                _ => panic!("Wrong block type")
            },
            _ => panic!("Wrong block type")
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

impl Upsertable<u64> for Block {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &u64) {
        match self {
            &mut Block::Int64Sparse(ref mut b) => b.multi_upsert(offsets, *val),
            _ => panic!("Wrong block type")
        }
    }

    fn upsert(&mut self, data : &Block) {
        match self {
            &mut Block::Int64Sparse(ref mut b) => match data {
                &Block::Int64Sparse(ref c) => b.upsert(c),
                _ => panic!("Wrong block")
            },
            _ => panic!("Wrong block type")
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

impl Upsertable<u32> for Block {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &u32) {
        match self {
            &mut Block::Int32Sparse(ref mut b) => b.multi_upsert(offsets, *val),
            _ => panic!("Wrong block type")
        }
    }

    fn upsert(&mut self, data : &Block) {
        match self {
            &mut Block::Int32Sparse(ref mut b) => match data {
                &Block::Int32Sparse(ref c) => b.upsert(c),
                _ => panic!("Wrong block type")
            },
            _ => panic!("Wrong block type")
        }
    }
}

impl Scannable<u16> for Block {
    fn scan(&self, op: ScanComparison, val: &u16, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::Int16Sparse(ref b) => b.scan(op, val, scan_consumer),
            _ => println!("Unrecognized u16 block type")
        }
    }
}

impl Upsertable<u16> for Block {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &u16) {
        match self {
            &mut Block::Int16Sparse(ref mut b) => b.multi_upsert(offsets, *val),
            _ => panic!("Wrong block type")
        }
    }

    fn upsert(&mut self, data : &Block) {
        match self {
            &mut Block::Int16Sparse(ref mut b) => match data {
                &Block::Int16Sparse(ref c) => b.upsert(c),
                _ => panic!("Wrong block type")
            },
            _ => panic!("Wrong block type")
        }
    }
}

impl Scannable<u8> for Block {
    fn scan(&self, op: ScanComparison, val: &u8, scan_consumer: &mut BlockScanConsumer) {
        match self {
            &Block::Int8Sparse(ref b) => b.scan(op, val, scan_consumer),
            _ => println!("Unrecognized u8 block type")
        }
    }
}

impl Upsertable<u8> for Block {
    fn multi_upsert(&mut self, offsets : &Vec<u32>, val : &u8) {
        match self {
            &mut Block::Int8Sparse(ref mut b) => b.multi_upsert(offsets, *val),
            _ => panic!("Wrong block type")
        }
    }

    fn upsert(&mut self, data : &Block) {
        match self {
            &mut Block::Int8Sparse(ref mut b) => match data {
                &Block::Int8Sparse(ref c) => b.upsert(c),
                _ => panic!("Wrong block type")
            },
            _ => panic!("Wrong block type")
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

    pub fn delete(&mut self, offsets: &Vec<u32>) {
        // Because the structure is bit more complex here, lets just be naive and rewrite the str_data while updating index data?

        let mut new_index_data:Vec<(u32, usize)> = Vec::new();
        let mut new_str_data:Vec<u8> = Vec::new();

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while data_index < self.index_data.len() {
            let cur_index = self.index_data[data_index];
            let cur_offset = cur_index.0;

            while offsets_index < offsets.len() && offsets[offsets_index] < cur_offset {
                offsets_index += 1;
            }

            // The next offset to remove is somewhere in front, so lets copy this entry
            if offsets_index == offsets.len() || offsets[offsets_index] > cur_offset {

                let end_str_index = if data_index == self.index_data.len()-1 {
                    self.str_data.len()
                } else {
                    self.index_data[data_index+1].1
                };

                let last_index = new_str_data.len();
                let cur_str_data = &self.str_data[cur_index.1..end_str_index];

                new_index_data.push((cur_index.0, last_index));
                new_str_data.extend_from_slice(cur_str_data);
            }

            data_index += 1;
        }

        self.index_data = new_index_data;
        self.str_data = new_str_data;
    }

    pub fn multi_upsert(&mut self, offsets: &Vec<u32>, v: &[u8]) {
        // Because the structure is bit more complex here, lets just be naive and rewrite the str_data while updating index data?

        let mut new_index_data:Vec<(u32, usize)> = Vec::new();
        let mut new_str_data:Vec<u8> = Vec::new();

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while offsets_index < offsets.len() || data_index < self.index_data.len() {
            let last_str_index = new_str_data.len();

            let cur_offset = if data_index < self.index_data.len() {
                self.index_data[data_index].0
            } else {
                offsets[offsets_index]
            };

            if offsets_index < offsets.len() && offsets[offsets_index] <= cur_offset {
                // Update/insert the value
                new_index_data.push((offsets[offsets_index], last_str_index));
                new_str_data.extend_from_slice(v);

                if offsets[offsets_index] == cur_offset {
                    // If we did update rather then insert to a non-existing entry
                    data_index += 1;
                }

                offsets_index += 1;
            } else if data_index < self.index_data.len() {
                let cur_index = self.index_data[data_index];

                // Copy from existing block
                let end_str_index = if data_index == self.index_data.len()-1 {
                    self.str_data.len()
                } else {
                    self.index_data[data_index+1].1
                };

                let cur_str_data = &self.str_data[cur_index.1..end_str_index];
                new_index_data.push((cur_index.0, last_str_index));
                new_str_data.extend_from_slice(cur_str_data);

                data_index += 1;
            }
        }

        self.index_data = new_index_data;
        self.str_data = new_str_data;
    }

    pub fn upsert(&mut self, data : &StringBlock) {

    }

    pub fn move_data(&mut self, target : &mut StringBlock, scan_consumer : &BlockScanConsumer) {
        let temp_block = self.filter_scan_results(scan_consumer);
        target.upsert(&temp_block);
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

    pub fn delete(&mut self, offsets: &Vec<u32>) {
        let mut indexes:Vec<usize> = Vec::new();

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while offsets_index < offsets.len() && data_index < self.data.len() {
            let target_offset = offsets[offsets_index];
            while data_index < self.data.len() && self.data[data_index].0 < target_offset {
                data_index += 1;
            }

            if data_index < self.data.len() && self.data[data_index].0 == target_offset {
                indexes.push(data_index);
                data_index += 1;
            }

            // Move on regardless
            offsets_index += 1;
        }

        indexes.reverse();
        for i in indexes {
            self.data.remove(i);
        }
    }

    pub fn move_data(&mut self, target : &mut TSparseBlock<T>, scan_consumer : &BlockScanConsumer) {
        let temp_block = self.filter_scan_results(scan_consumer);
        target.upsert(&temp_block);
    }


    // Put specific value to multiple columns
    pub fn multi_upsert(&mut self, offsets: &Vec<u32>, v: T) {
        let mut indexes:Vec<usize> = Vec::new();

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while offsets_index < offsets.len() {
            let target_offset = offsets[offsets_index];

            // Forward the self.data position to current offset
            while data_index < self.data.len() && self.data[data_index].0 < target_offset {
                data_index += 1;
            }

            //self.data.insert()
            if data_index < self.data.len() {
                if self.data[data_index].0 == target_offset {
                    let record = &mut self.data[data_index];
                    record.1 = v.to_owned();
                } else {
                    // insert
                    self.data.insert(data_index, (target_offset, v.to_owned()));
                }
            } else {
                // append
                self.data.push((target_offset, v.to_owned()));
            }

            // Move on regardless
            offsets_index += 1;
        }

    }

    // Upsert specific values
    pub fn upsert(&mut self, upsert_data : &TSparseBlock<T>) {
        let mut indexes:Vec<usize> = Vec::new();

        let mut offsets_index = 0 as usize;
        let mut data_index = 0 as usize;

        while offsets_index < upsert_data.data.len() {
            let target_offset = upsert_data.data[offsets_index].0;

            // Forward the self.data position to current offset
            while data_index < self.data.len() && self.data[data_index].0 < target_offset {
                data_index += 1;
            }

            //self.data.insert()
            if data_index < self.data.len() {
                if self.data[data_index].0 == target_offset {
                    let record = &mut self.data[data_index];
                    record.1 = upsert_data.data[offsets_index].1.to_owned();
                } else {
                    // insert
                    self.data.insert(data_index, (target_offset, upsert_data.data[offsets_index].1.to_owned()));
                }
            } else {
                // append
                self.data.push((target_offset, upsert_data.data[offsets_index].1.to_owned()));
            }

            // Move on regardless
            offsets_index += 1;
        }

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


// This is not utf-8 aware
fn strings_ne_match(s1 : &[u8], op : &ScanComparison, s2 : &[u8]) -> bool {
    for i in 0..cmp::min(s1.len(), s2.len()) {
        if s1[i] == s2[i] {
            // just continue
        } else {
            match op {
                &ScanComparison::LtEq => return s1[i] < s2[i],
                &ScanComparison::Lt => return s1[i] < s2[i],
                &ScanComparison::Gt => return s1[i] > s2[i],
                &ScanComparison::GtEq => return s1[i] > s2[i],
                //_ => println!("Only <, <=, >=, > matches are handled here..."); return false
                _ => return false
            }
        }
    }

    // The shorter string was a substring of the longer one...
    match op {
        &ScanComparison::LtEq => return s1.len() < s2.len(),
        &ScanComparison::Lt => return s1.len() < s2.len(),
        &ScanComparison::Gt => return s1.len() > s2.len(),
        &ScanComparison::GtEq => return s1.len() > s2.len(),
        _ => return false
//        _ => println!("Only <, <=, >=, > matches are handled here...")
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
//                    ScanComparison::Lt =>
                    ScanComparison::NotEq => {
                        if size != val.len() || val != &self.str_data[prev_position..position] { scan_consumer.matching_offsets.push(prev_offset) }
                    },
                    _ => if strings_ne_match(&self.str_data[prev_position..position], &op, val) { scan_consumer.matching_offsets.push(prev_offset) }
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
                _ => if strings_ne_match(&self.str_data[prev_position..position], &op, val) { scan_consumer.matching_offsets.push(prev_offset) }
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
fn delete_sparse_block() {
    let mut input_block = Int32SparseBlock {
        data: vec![
            (1, 100),
            (2, 200),
            (3, 300),
            (6, 600),
            (8, 800),
            (11, 1100)
        ]
    };

    let mut expected_block = Int32SparseBlock {
        data: vec![
            (1, 100),
            (8, 800),
            (11, 1100)
        ]
    };

    let offsets = vec![2,3,6];
    input_block.delete(&offsets);

    assert_eq!(expected_block, input_block);
}

#[test]
fn delete_string_block() {
    let mut input_block = StringBlock::new();
    input_block.append(1, "foo".as_bytes());
    input_block.append(2, "bar".as_bytes());
    input_block.append(13, "snafu".as_bytes());


    let mut expected_block = StringBlock::new();
    expected_block.append(1, "foo".as_bytes());
    expected_block.append(13, "snafu".as_bytes());

    let offsets = vec![2,3,6];
    input_block.delete(&offsets);

    assert_eq!(expected_block, input_block);
}


#[test]
fn multi_upsert_string_block() {
    let mut input_block = StringBlock::new();
    input_block.append(1, "foo".as_bytes());
    input_block.append(2, "bar".as_bytes());
    input_block.append(13, "snafu".as_bytes());


    let mut expected_block = StringBlock::new();
    expected_block.append(0, "lol".as_bytes());
    expected_block.append(1, "foo".as_bytes());
    expected_block.append(2, "lol".as_bytes());
    expected_block.append(3, "lol".as_bytes());
    expected_block.append(13, "snafu".as_bytes());

    let offsets = vec![0,2,3];
    input_block.multi_upsert(&offsets, "lol".as_bytes());

    assert_eq!(expected_block, input_block);
}

#[test]
fn multi_upsert_sparse_block() {
    let mut input_block = Int32SparseBlock {
        data: vec![
            (1, 100),
            (8, 800),
            (11, 1100)
        ]
    };

    let mut expected_block = Int32SparseBlock {
        data: vec![
            (0, 9999),
            (1, 9999),
            (2, 9999),
            (8, 800),
            (11, 1100),
            (12, 9999)
        ]
    };

    let offsets = vec![0,1,2,12];
    input_block.multi_upsert(&offsets, 9999);

    assert_eq!(expected_block, input_block);

}

#[test]
fn upsert_sparse_block() {
    let mut input_block = Int32SparseBlock {
        data: vec![
            (1, 100),
            (8, 800),
            (11, 1100)
        ]
    };

    let mut expected_block = Int32SparseBlock {
        data: vec![
            (1, 101),
            (2, 202),
            (8, 800),
            (11, 1100),
        ]
    };

    let upsert_data = Int32SparseBlock {
        data: vec![
            (1,101),
            (2,202)
        ]
    };

    input_block.upsert(&upsert_data);

    assert_eq!(expected_block, input_block);

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
