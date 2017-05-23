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

    pub fn add_column(&mut self, data_type: BlockType, name: String) -> Column {
        let new_col = Column { data_type: data_type, name: name };
        self.columns.push(new_col.to_owned());
        new_col
    }

    pub fn create_partition<'a>(&self) -> Partition {
        let mut blocks = Vec::new();
        for col in &self.columns {
            blocks.push(Block::create_block(&col.data_type));
        }

        Partition { min_ts: 0, max_ts: 0, blocks : blocks }
    }

}

