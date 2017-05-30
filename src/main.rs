#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bincode;
extern crate nanomsg;

extern crate rand;
use rand::Rng;
use std::time::Instant;


pub mod catalog;
pub mod scan;
pub mod partition;
pub mod int_blocks;
pub mod api;
pub mod manager;
pub mod nanomsg_endpoint;

use nanomsg_endpoint::start_endpoint;

use catalog::Catalog;
use catalog::Column;
use catalog::BlockType;

use int_blocks::Block;
use int_blocks::Scannable;
use int_blocks::Int64DenseBlock;
use int_blocks::Int64SparseBlock;
use int_blocks::Int32SparseBlock;

use partition::Partition;

use scan::{BlockScanConsumer};
use api::{ScanResultMessage, ScanComparison};

use api::InsertMessage;

use manager::Manager;

static TEST_COLS_SPARSE_I64: u32 = 20;
static TEST_ROWS_PER_PACKAGE: u32 = 100000;

fn create_message(ts_base : u64) -> InsertMessage {
    let mut rng = rand::thread_rng();

    let mut pseudo_ts = ts_base * 1000000;

    let mut blocks : Vec<Block> = Vec::new();
    let mut col_types : Vec<(u32, BlockType)> = Vec::new();

    for col_no in 0..TEST_COLS_SPARSE_I64 + 2 {
        let block : Block;
        let block_type : BlockType;

        match col_no {
            0 => {
                block = Block::Int64Dense(Int64DenseBlock{
                    data: (0..TEST_ROWS_PER_PACKAGE).into_iter().map(|x| pseudo_ts + x as u64 * 1000 ).collect()
                });
                block_type = BlockType::Int64Dense;
            },
            1 => {
                block = Block::Int64Dense(Int64DenseBlock{
                    data: (0..TEST_ROWS_PER_PACKAGE).into_iter().map(|x| x as u64 % 3 ).collect()
                });
                block_type = BlockType::Int64Dense;
            },
            _ => {
                block = Block::Int64Sparse(Int64SparseBlock{
                    data: (0..TEST_ROWS_PER_PACKAGE).into_iter().map(|x| (x as u32, rng.gen::<u64>())).collect()
                });
                block_type = BlockType::Int64Sparse;
            }
        }

        blocks.push(block);
        col_types.push((col_no, block_type));
    }


    InsertMessage {
        row_count: TEST_ROWS_PER_PACKAGE,
        col_count: blocks.len() as u32,
        col_types: col_types,
        blocks: blocks
    }

}

fn prepare_catalog(manager : &mut Manager) {
    manager.catalog.add_column(BlockType::Int64Dense, String::from("ts"));
    manager.catalog.add_column(BlockType::Int64Dense, String::from("source"));
    for x in 0..TEST_COLS_SPARSE_I64 {
        manager.catalog.add_column(BlockType::Int64Sparse, format!("col_{}", x));
    }

    println!("Following columns are defined");
    for col in manager.catalog.columns.iter_mut() {
        println!("Column: {} of type {:?}", col.name, col.data_type);
    }
}

fn prepare_fake_data(manager : &mut Manager) {
    let create_duration = Instant::now();

    let mut cur_ts : u64 = 149500000;

    let mut total_count : usize = 0;

    for iter in 0..50 {
        let msg = create_message(cur_ts + iter*17);
        total_count += msg.row_count as usize;
        manager.insert(&msg);
    }

    manager.dump_in_mem_partition();

    println!("Creating {} records took {:?}", total_count, create_duration.elapsed());
}

fn prepare_demo_scan(manager : &mut Manager) {

    let scan_duration = Instant::now();

    let mut total_matched = 0;
    let mut total_materialized = 0;

    for part_info in &manager.catalog.available_partitions {
        let scanned_block = manager.load_block(&part_info, 7);
        let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
        scanned_block.scan(ScanComparison::LtEq, &(1363258435234989944 as u64), &mut consumer);

        let mut scan_msg = ScanResultMessage::new();
        consumer.materialize(&manager, part_info, &vec![0,1,3,4,5], &mut scan_msg);

        total_materialized += scan_msg.row_count;
        total_matched += consumer.matching_offsets.len();
    }
    println!("Scanning and matching/materializing {}/{} elements took {:?}", total_matched, total_materialized, scan_duration.elapsed());
}

fn main() {

    let mut manager = Manager::new(String::from("/tmp/hyena"));

    prepare_catalog(&mut manager);
//    prepare_fake_data(&mut manager);
//    prepare_demo_scan(&mut manager);

    start_endpoint(&mut manager);


//    let ref scanned_block = partition.blocks[4];
//    let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
//    scanned_block.scan(ScanComparison::LtEq, &(1363258435234989944 as u64), &mut consumer);

//    println!("Scanning and matching {} elements took {:?}", consumer.matching_offsets.len(), scan_duration.elapsed());

    //    println!("Elapsed: {} ms",
//             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

}
