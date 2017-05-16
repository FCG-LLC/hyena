extern crate rand;
use rand::Rng;
use std::time::Instant;


pub mod catalog;
pub mod scan;
pub mod partition;
pub mod int_blocks;

use catalog::Catalog;
use catalog::Column;
use catalog::BlockType;
use partition::Partition;

static TEST_COLS_DENSE_I64: i32 = 20;
static TEST_ROWS_PER_PART: i32 = 1000000;

fn fill_partition(part : &mut Partition) {
    let mut rng = rand::thread_rng();

    for x in 0..TEST_COLS_DENSE_I64 {
        let mut col = &mut part.blocks[0];
        col.append(rng.gen::<u64>());
    }
}

fn main() {
    let start = Instant::now();

    let catalog : &mut Catalog = &mut Catalog { columns : Vec::new() };

    for x in 0..TEST_COLS_DENSE_I64 {
        catalog.add_column(BlockType::Int64Dense, format!("col_{}", x));
    }

    let mut partition : Partition = catalog.create_partition();
    fill_partition(&mut partition);


    println!("Following columns are defined");
    for col in catalog.columns.iter_mut() {
        println!("Column: {} of type {:?}", col.name, col.data_type);
    }

    let elapsed = start.elapsed();
    println!("{:?}", elapsed);
    println!("Elapsed: {} ms",
             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

}
