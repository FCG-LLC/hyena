use catalog::Catalog;
use catalog::BlockType;
use catalog::PartitionInfo;
use partition::{Partition, PartitionMetadata};
use int_blocks::{Block, Int32SparseBlock, Int64DenseBlock, Int64SparseBlock, StringBlock};
use api::InsertMessage;

use bincode::{serialize, deserialize, Infinite};
use serde::ser::{Serialize};
use std::fs;
use std::error::Error;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
use std::io::BufReader;

pub struct Manager {
    pub db_home: String,
    pub catalog: Catalog,
    pub current_partition: Partition
}

// To be used only within extremely limited context
pub struct BlockCache {
    pub partition_info : PartitionInfo,
    pub cache:Vec<(u32, Block)>
}

impl BlockCache {
    pub fn new(partition_info : &PartitionInfo) -> BlockCache {
        BlockCache {
            partition_info: (partition_info.to_owned()),
            cache: Vec::new()
        }
    }

    pub fn cache_block(&mut self, block : Block, block_index: u32) {
        self.cache.push((block_index, block));
    }

    pub fn cached_block_maybe<'a>(&'a self, block_index: u32) -> Option<&'a Block> {
        for tuple in &self.cache {
            let cached_index = tuple.0;
            let ref cached_block = tuple.1;
            if block_index == cached_index {
                return Option::from(cached_block);
            }
        }
        Option::None
    }

//    fn fetch_block(&'a self, block_index : u32) -> Block {
//        manager.load_block(&self.partition_info, block_index)
//    }

//    pub fn get_cached_or_load(&mut self, manager : &Manager, block_index : u32) -> &Block {
//        let block_option = self.find_cached_block(block_index);
//
//        match block_option {
//            None => {
//                let block = manager.load_block(&self.partition_info, block_index);
//                self.cache.push((block_index, &block));
//                return &block;
//            }
//            Some(ref x) => {
//                return x;
//            }
//        }
//    }
}

fn ensure_partition_is_current(catalog: &Catalog, part: &mut Partition) {
    if part.blocks.len() < catalog.columns.len() {
        for block_no in part.blocks.len()..catalog.columns.len() {
            part.blocks.push(Block::create_block(&catalog.columns[block_no].data_type));
        }
    }
}

fn save_data<T: Serialize>(path : &String, data : &T) {
    let mut file = File::create(path).expect("Unable to create file");

    let bytes:Vec<u8> = serialize(data, Infinite).unwrap();
    file.write_all(&bytes).unwrap();
}

fn read_block(path : &String) -> Block {
    println!("Reading block {}", path);

    let file = File::open(path).unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut buf: Vec<u8> = Vec::new();
    buf_reader.read_to_end(&mut buf).unwrap();

    deserialize(&buf[..]).unwrap()
}


impl Manager {
    pub fn new(db_home:String) -> Manager {
        Manager { db_home: db_home, catalog: Catalog::new(), current_partition: Partition::new() }
    }

    pub fn add_column(&mut self, data_type: BlockType, name: String) {
        ensure_partition_is_current(&self.catalog, &mut self.current_partition);

        println!("Adding column <{}> of type {:?}", name, data_type);
        let col = self.catalog.add_column(data_type, name);
        self.current_partition.blocks.push(Block::create_block(&col.data_type));
    }

    pub fn find_partition_info(&self, partition_id: u64) -> PartitionInfo {
        for part in &self.catalog.available_partitions {
            if part.id == partition_id {
                return part.to_owned();
            }
        }

        println!("Could not find partition_id {}", partition_id);

        // FIXME -> RESULT
//        fail!("NOPE");
        let pi = PartitionInfo{
            min_ts: 0,
            max_ts: 0,
            id: 0,
            location: String::from("dupa")
        };
        pi
    }

    pub fn insert(&mut self, msg : &InsertMessage) {
        println!("Inserting a message of {} records", msg.row_count);

        // TODO: validate columns - their types and if they exist

        // TODO: for sparse sets we could add assertion that order of offsets is monotonically growing

        ensure_partition_is_current(&self.catalog, &mut self.current_partition);

        let current_offset = self.current_partition.blocks[0].len();

        for col_no in 0..msg.col_count {
            let catalog_col_no = msg.col_types[col_no as usize].0;
            let input_block = &msg.blocks[col_no as usize];
            let output_block = &mut self.current_partition.blocks[catalog_col_no as usize];

            match input_block  {
                &Block::Int64Dense(ref in_block) => {
                    match output_block {
                        &mut Block::Int64Dense(ref mut out_block) => {
                            assert_eq!(in_block.data.len(), msg.row_count as usize);
                            out_block.data.extend(&in_block.data);
                        },
                        _ => panic!("Non matching blocks")
                    }
                },
                &Block::Int64Sparse(ref in_block) => {
                    match output_block {
                        &mut Block::Int64Sparse(ref mut out_block) => {
                            for pair in &in_block.data {
                                assert!(pair.0 < msg.row_count);

                                out_block.data.push((pair.0 + current_offset as u32, pair.1));
                            }
                        },
                        _ => panic!("Non matching blocks")
                    }
                },
                &Block::Int32Sparse(ref in_block) => {
                    match output_block {
                        &mut Block::Int32Sparse(ref mut out_block) => {
                            for pair in &in_block.data {
                                assert!(pair.0 < msg.row_count);

                                out_block.data.push((pair.0 + current_offset as u32, pair.1));
                            }
                        },
                        _ => panic!("Non matching blocks")
                    }
                },
                &Block::Int16Sparse(ref in_block) => {
                    match output_block {
                        &mut Block::Int16Sparse(ref mut out_block) => {
                            for pair in &in_block.data {
                                assert!(pair.0 < msg.row_count);

                                out_block.data.push((pair.0 + current_offset as u32, pair.1));
                            }
                        },
                        _ => panic!("Non matching blocks")
                    }
                },
                &Block::Int8Sparse(ref in_block) => {
                    match output_block {
                        &mut Block::Int8Sparse(ref mut out_block) => {
                            for pair in &in_block.data {
                                assert!(pair.0 < msg.row_count);

                                out_block.data.push((pair.0 + current_offset as u32, pair.1));
                            }
                        },
                        _ => panic!("Non matching blocks")
                    }
                },
                &Block::StringBlock(ref in_block) => {
                    match output_block {
                        &mut Block::StringBlock(ref mut out_block) => {
                            for (index, pair) in in_block.index_data.iter().enumerate() {
                                assert!(pair.0 < msg.row_count);

                                let offset = pair.0;
                                let position = pair.1;

                                out_block.index_data.push((pair.0 + current_offset as u32, out_block.str_data.len()));

                                let end_position = if index < in_block.index_data.len()-1 {
                                    in_block.index_data[index+1].1
                                } else {
                                    in_block.str_data.len()
                                };

                                out_block.str_data.extend(&in_block.str_data[position..end_position])
                            }
                        },
                        _ => {
                            panic!("Non matching blocks");
                        }
                    }
                },
           }
        }

        if self.current_partition.blocks[0].len() > 200000 {
            self.dump_in_mem_partition();
        }
    }

    pub fn catalog_path(&self) -> String {
        let catalog_file_name = self.db_home.to_owned() + "/catalog.bin";
        catalog_file_name
    }

    pub fn partition_path(&self, metadata : &PartitionMetadata) -> String {
        let mut partition_file_name = self.db_home.to_owned() + "/partitions/";

        let min_ts = metadata.min_ts / 1000000;

        for i in vec![10000000, 100000, 100] {
            let ts = (min_ts / i)*i;
            partition_file_name += &format!("{}/", ts);
        }

        partition_file_name += &metadata.min_ts.to_string();

        partition_file_name
    }

    pub fn reload_catalog(&mut self) {
        let file = File::open(self.catalog_path()).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut buf: Vec<u8> = Vec::new();
        buf_reader.read_to_end(&mut buf).unwrap();

        self.catalog = deserialize(&buf[..]).unwrap();
    }

    pub fn store_catalog(&self) {
        fs::create_dir_all(&self.db_home);

        save_data(&self.catalog_path(), &self.catalog);
    }

    pub fn store_partition(&self, part : &Partition) -> String {
        let part_path = self.partition_path(&part.metadata);

        fs::create_dir_all(&part_path);

        save_data(&format!("{}/metadata.bin", part_path), &part.metadata);
        for block_index in &part.metadata.existing_blocks {
            save_data(&format!("{}/block_{}.bin", part_path, block_index), &part.blocks[*block_index as usize]);
        }

        // metadata
        // each non-empty block

        println!("Saved partition: {}", part_path);

        part_path
    }

    pub fn load_block(&self, pinfo : &PartitionInfo, block_index : u32) -> Block {
        let part_path = &pinfo.location;

        // TODO: block might not exist and it should be considered OK in many circumstances
        read_block(&format!("{}/block_{}.bin", part_path, block_index))
    }

    pub fn dump_in_mem_partition(&mut self) {
        if self.current_partition.blocks.is_empty() {
            println!("Cannot dump empty partition");
            return
        }

        println!("Dumping in memory partition having {} records", self.current_partition.blocks[0].len());
        self.current_partition.prepare();
        let stored_path = self.store_partition(&self.current_partition);
        self.catalog.available_partitions.push(PartitionInfo {
            min_ts: self.current_partition.metadata.min_ts,
            max_ts: self.current_partition.metadata.max_ts,
            id: self.current_partition.metadata.id,
            location: stored_path
        });

        self.current_partition = Partition::new();
    }

}



fn create_catalog<'a>() -> Manager {
    let mut manager = Manager::new(String::from("/tmp/hyena"));

    manager.catalog.add_column(BlockType::Int64Dense, String::from("ts"));
    manager.catalog.add_column(BlockType::Int64Dense, String::from("source"));
    manager.catalog.add_column(BlockType::Int64Sparse, String::from("int_01"));
    manager.catalog.add_column(BlockType::Int32Sparse, String::from("int_02"));
    manager.catalog.add_column(BlockType::String, String::from("str"));

    manager.store_catalog();

    manager
}

//#[test]
//fn it_saves_and_loads_catalog() {
//    let manager = create_catalog();
//
//    let manager2 = &mut Manager::new(String::from("/tmp/hyena"));
//    manager2.reload_catalog();
//
//    assert_eq!(manager2.catalog, manager.catalog);
//}

#[test]
fn it_inserts_and_dumps_data_smoke_test() {
    let mut manager = create_catalog();

    let base_ts = 1495493600 as u64 * 1000000;
    let insert_msg = InsertMessage {
        row_count: 4,
        col_count: 5,
        col_types: vec![(0, BlockType::Int64Dense), (1, BlockType::Int64Dense), (2, BlockType::Int64Sparse), (3, BlockType::Int32Sparse), (4, BlockType::String)],
        blocks: vec![
            Block::Int64Dense(Int64DenseBlock{
                data: vec![base_ts, base_ts+1000, base_ts+2000, base_ts+3000]
            }),
            Block::Int64Dense(Int64DenseBlock{
                data: vec![0, 0, 1, 2]
            }),
            Block::Int64Sparse(Int64SparseBlock{
                data: vec![(0, 100), (1, 200)]
            }),
            Block::Int32Sparse(Int32SparseBlock{
                data: vec![(2, 300), (3, 400)]
            }),
            Block::StringBlock(StringBlock{
                index_data: vec![(1,0),(2,3)],
                str_data: "foobar".as_bytes().to_vec()
            })
        ]
    };

    manager.insert(&insert_msg);

    manager.dump_in_mem_partition();
}
