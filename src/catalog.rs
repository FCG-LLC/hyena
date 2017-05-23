use int_blocks::Int64DenseBlock;
use int_blocks::Int64SparseBlock;
use int_blocks::Int32SparseBlock;
use partition::Partition;
use int_blocks::Block;

#[repr(u8)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum BlockType {
    Int64Dense,
    Int64Sparse,
    Int32Sparse
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Column {
    pub data_type: BlockType,
    pub name: String
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Catalog {
    pub columns: Vec<Column>,
    pub available_partitions: Vec<PartitionInfo>
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PartitionInfo {
    pub min_ts: u64,
    pub max_ts: u64,
    pub location: String
}

pub fn push_block(col: &Column, blocks: &mut Vec<Block>) {
    match col.data_type {
        BlockType::Int64Dense => blocks.push(Block::Int64Dense(Int64DenseBlock{data : Vec::new()})),
        BlockType::Int64Sparse => blocks.push(Block::Int64Sparse(Int64SparseBlock{data : Vec::new()})),
        BlockType::Int32Sparse => blocks.push(Block::Int32Sparse(Int32SparseBlock{data : Vec::new()})),
        _ => println!("Not suppported"),
    }
}


impl Catalog {
//    pub fn column_index(&self, name: &String) -> u32 {
//        0
//    }
    pub fn new() -> Catalog {
        Catalog {
            columns: Vec::new(),
            available_partitions: Vec::new()
        }
    }

    pub fn add_column(&mut self, data_type: BlockType, name: String) {
        let new_col = Column { data_type: data_type, name: name };
        self.columns.push(new_col);
    }

    pub fn create_partition<'a>(&self) -> Partition {
        let mut blocks = Vec::new();
        for col in &self.columns {
            push_block(col, &mut blocks);
        }

        Partition { min_ts: 0, max_ts: 0, blocks : blocks }
    }

}

