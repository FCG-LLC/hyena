use int_blocks::Int64DenseBlock;
use int_blocks::Int64SparseBlock;
use int_blocks::Int32SparseBlock;
use partition::Partition;
use int_blocks::Block;

#[repr(u8)]
#[derive(Debug,Clone,PartialEq)]
pub enum BlockType {
    Int64Dense,
    Int64Sparse,
    Int32Sparse
}

#[derive(Debug)]
pub struct Column {
    pub data_type: BlockType,
    pub name: String
}

#[derive(Debug)]
pub struct Catalog {
    pub columns: Vec<Column>
}

impl Catalog {
//    pub fn column_index(&self, name: &String) -> u32 {
//        0
//    }

    pub fn add_column(&mut self, data_type: BlockType, name: String) {
        self.columns.push(Column { data_type: data_type, name: name });
    }

    pub fn create_partition<'a>(&self) -> Partition {
        let mut blocks = Vec::new();
        for col in &self.columns {
            match col.data_type {
                BlockType::Int64Dense => blocks.push(Block::Int64Dense(Int64DenseBlock{data : Vec::new()})),
                BlockType::Int64Sparse => blocks.push(Block::Int64Sparse(Int64SparseBlock{data : Vec::new()})),
                BlockType::Int32Sparse => blocks.push(Block::Int32Sparse(Int32SparseBlock{data : Vec::new()})),
                _ => println!("Not suppported"),
            }

        }

        Partition { min_ts: 0, max_ts: 0, blocks : blocks }
    }
}

