#[macro_use]
extern crate serde_derive;
extern crate bincode;

extern crate rand;
use rand::Rng;
use std::time::Instant;


pub mod catalog;
pub mod scan;
pub mod partition;
pub mod int_blocks;
pub mod api;
pub mod nanomsg_endpoint;

use catalog::Catalog;
use catalog::Column;
use catalog::BlockType;

use int_blocks::Block;
use int_blocks::Scannable;

use partition::Partition;

use scan::BlockScanConsumer;
use scan::ScanComparison;

static TEST_COLS_SPARSE_I64: i32 = 20;
static TEST_ROWS_PER_PART: i32 = 1000;

fn fill_partition(part : &mut Partition) {
    let mut rng = rand::thread_rng();

    let mut row_index = 0;

    let mut pseudo_ts = 1495000000 as u64  * 1000000;
    let ts_step = 100;

    for x in 0..TEST_ROWS_PER_PART {
        let mut col_index = 0;
        for col in part.blocks.iter_mut() {
            match col {
                &mut Block::Int64Dense(ref mut b) => {
                    match col_index {
                        0 => b.append(pseudo_ts),     // ts
                        1 => b.append(pseudo_ts % 3), // source
                        _ => b.append(rng.gen::<u64>())
                    }
                },
                &mut Block::Int64Sparse(ref mut b) => b.append(row_index, rng.gen::<u64>()),
                &mut Block::Int32Sparse(ref mut b) => b.append(row_index, rng.gen::<u32>()),
                _ => println!("Oopsie")
            }

            col_index += 1;
        }

        row_index += 1;
    }
}

fn main() {
    let create_duration = Instant::now();

    let catalog : &mut Catalog = &mut Catalog { columns : Vec::new() };


    catalog.add_column(BlockType::Int64Dense, String::from("ts"));
    catalog.add_column(BlockType::Int64Dense, String::from("source"));
    for x in 0..TEST_COLS_SPARSE_I64 {
        catalog.add_column(BlockType::Int64Sparse, format!("col_{}", x));
    }

    let mut partition : Partition = catalog.create_partition();

    println!("Following columns are defined");
    for col in catalog.columns.iter_mut() {
        println!("Column: {} of type {:?}", col.name, col.data_type);
    }

    fill_partition(&mut partition);

    println!("Creating data took {:?}", create_duration.elapsed());

    let scan_duration = Instant::now();
    let ref scanned_block = partition.blocks[4];
    let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
    scanned_block.scan(ScanComparison::LtEq, &(1363258435234989944 as u64), &mut consumer);

    println!("Scanning and matching {} elements took {:?}", consumer.matching_offsets.len(), scan_duration.elapsed());

    //    println!("Elapsed: {} ms",
//             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

}
