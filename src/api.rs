use bincode::{serialize, deserialize, Infinite};
use catalog::BlockType;
use manager::Manager;
use int_blocks::{Block, Int32SparseBlock, Int64DenseBlock, Int64SparseBlock};

#[derive(Serialize, Deserialize, PartialEq)]
pub struct InsertMessage {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
    pub blocks : Vec<Block> // This can be done right now only because blocks are so trivial
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct ScanResultMessage {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
    pub blocks : Vec<Block> // This can be done right now only because blocks are so trivial
}

pub fn insert_serialized_request(manager: &mut Manager, buf : &Vec<u8>) {
    let msg : InsertMessage = deserialize(&buf[..]).unwrap();

    manager.insert(&msg);
}

#[test]
fn it_works() {
    let mut test_msg:Vec<u8> = vec![];

    let base_ts = 1495490000 * 1000000;

    let insert_msg = InsertMessage {
        row_count: 3,
        col_count: 5,
        col_types: vec![(0, BlockType::Int64Dense), (1, BlockType::Int64Dense), (2, BlockType::Int64Sparse), (4, BlockType::Int64Sparse)],
        blocks: vec![
            Block::Int64Dense(Int64DenseBlock{
                data: vec![base_ts, base_ts+1000, base_ts+2000]
            }),
            Block::Int64Dense(Int64DenseBlock{
                data: vec![0, 0, 1, 2]
            }),
            Block::Int64Sparse(Int64SparseBlock{
                data: vec![(0, 100), (1, 200)]
            }),
            Block::Int64Sparse(Int64SparseBlock{
                data: vec![(2, 300), (3, 400)]
            }),
        ]
    };
    
    test_msg.extend(serialize(&insert_msg, Infinite).unwrap());

    println!("In test {:?}", test_msg);
//    insert_serialized_request(&test_msg);


}

