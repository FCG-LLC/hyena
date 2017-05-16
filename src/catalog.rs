use int_blocks::Int64DenseBlock;
use int_blocks::Int64SparseBlock;
use partition::Partition;

//#[repr(u8)]
#[derive(Debug,Clone,PartialEq)]
pub enum BlockType {
    Int64Dense,
    Int64Sparse
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
    pub fn column_index(&self, name: &String) -> u32 {
        0
    }

    pub fn add_column(&mut self, data_type: BlockType, name: String) {
        self.columns.push(Column { data_type: data_type, name: name });
    }

    pub fn create_partition<'a>(&self) -> Partition {
        let mut blocks = Vec::new();
        for col in &self.columns {
            match col.data_type {
                BlockType::Int64Dense => blocks.push(Int64DenseBlock{data : Vec::new()}),
                _ => println!("Not suppported"),
            }

        }

        Partition { blocks : blocks }
    }
}

