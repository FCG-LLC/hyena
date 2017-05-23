use catalog::Catalog;
use catalog::BlockType;
use partition::Partition;

use bincode::{serialize, deserialize, Infinite};
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

impl Manager {
    pub fn new(db_home:String) -> Manager {
        Manager { db_home: db_home, catalog: Catalog::new(), current_partition: Partition::new() }
    }

    pub fn add_column(&mut self, data_type: BlockType, name: String) {
        self.catalog.add_column(data_type, name);
        push_block(&new_col, &mut self.current_partition.blocks);
    }

    pub fn get_in_memory_partition(&mut self) -> &mut Partition {
        if self.current_partition.blocks[0].len() > 1000000 {
            // dump the current partition and create new one

        }

        return &mut self.current_partition;
    }


    pub fn catalog_path(&self) -> String {
        let catalog_file_name = self.db_home.to_owned() + "/catalog.bin";
        catalog_file_name
    }

    pub fn reload_catalog(&mut self) {
        let file = File::open(self.catalog_path()).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut buf: Vec<u8> = Vec::new();
        buf_reader.read_to_end(&mut buf).unwrap();

        println!("Got {} bytes", buf.len());
        let cat:Catalog = deserialize(&buf[..]).unwrap();

    }

    pub fn store_catalog(&self) {
        fs::create_dir_all(&self.db_home);

        let mut file = File::create(self.catalog_path()).expect("Unable to create catalog file");

        let bytes:Vec<u8> = serialize(&self.catalog, Infinite).unwrap();
        file.write_all(&bytes).unwrap();

        println!("Catalog is {} bytes", bytes.len());
        let cc:Catalog = deserialize(&bytes).unwrap();
        println!("HMMM");


//        file.sync_all().unwrap();
    }
}

#[test]
fn it_saves_and_loads_data() {
    let manager = &mut Manager::new(String::from("/tmp/hyena"));

    manager.catalog.add_column(BlockType::Int64Dense, String::from("ts"));
    manager.catalog.add_column(BlockType::Int64Dense, String::from("source"));
    manager.catalog.add_column(BlockType::Int64Sparse, String::from("int_01"));
    manager.catalog.add_column(BlockType::Int32Sparse, String::from("int_02"));

    manager.store_catalog();

    let manager2 = &mut Manager::new(String::from("/tmp/hyena"));
    manager2.reload_catalog();

//    assert_eq!(manager2.catalog, manager.catalog);
}
