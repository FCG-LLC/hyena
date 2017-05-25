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

use catalog::Catalog;
use catalog::Column;
use catalog::BlockType;

use int_blocks::Block;
use int_blocks::Scannable;
use int_blocks::Int64DenseBlock;
use int_blocks::Int64SparseBlock;
use int_blocks::Int32SparseBlock;

use partition::Partition;

use scan::BlockScanConsumer;
use scan::ScanComparison;

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

//
//    for x in 0..TEST_ROWS_PER_PACKAGE {
//        let mut col_index = 0;
//        for col in part.blocks.iter_mut() {
//            match col {
//                &mut Block::Int64Dense(ref mut b) => {
//                    match col_index {
//                        0 => b.append(pseudo_ts),     // ts
//                        1 => b.append(pseudo_ts % 3), // source
//                        _ => b.append(rng.gen::<u64>())
//                    }
//                },
//                &mut Block::Int64Sparse(ref mut b) => b.append(row_index, rng.gen::<u64>()),
//                &mut Block::Int32Sparse(ref mut b) => b.append(row_index, rng.gen::<u32>()),
//                _ => println!("Oopsie")
//            }
//
//            col_index += 1;
//        }
//
//        row_index += 1;
//    }


    InsertMessage {
        row_count: TEST_ROWS_PER_PACKAGE,
        col_count: blocks.len() as u32,
        col_types: col_types,
        blocks: blocks
    }

}

fn main() {
    let create_duration = Instant::now();

    let mut total_count : usize = 0;

    let mut manager = Manager::new(String::from("/tmp/hyena"));

    manager.catalog.add_column(BlockType::Int64Dense, String::from("ts"));
    manager.catalog.add_column(BlockType::Int64Dense, String::from("source"));
    for x in 0..TEST_COLS_SPARSE_I64 {
        manager.catalog.add_column(BlockType::Int64Sparse, format!("col_{}", x));
    }

    println!("Following columns are defined");
    for col in manager.catalog.columns.iter_mut() {
        println!("Column: {} of type {:?}", col.name, col.data_type);
    }

    let mut cur_ts : u64 = 149500000;

    for iter in 0..100 {
        let msg = create_message(cur_ts + iter*17);
        total_count += msg.row_count as usize;
        manager.insert(&msg);
    }

    manager.dump_in_mem_partition();

    println!("Creating {} records took {:?}", total_count, create_duration.elapsed());

    let scan_duration = Instant::now();
//    let ref scanned_block = partition.blocks[4];
//    let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
//    scanned_block.scan(ScanComparison::LtEq, &(1363258435234989944 as u64), &mut consumer);

//    println!("Scanning and matching {} elements took {:?}", consumer.matching_offsets.len(), scan_duration.elapsed());

    //    println!("Elapsed: {} ms",
//             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

}
