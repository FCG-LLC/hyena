use std::time::Instant;

//extern crate catalog;

pub mod catalog;
pub mod scan;
pub mod partition;
pub mod int_blocks;

use catalog::Catalog;
use catalog::Column;
use catalog::BlockType;

static TEST_COLS_DENSE_I64: i32 = 20;

fn main() {
    let start = Instant::now();

    let catalog : &mut Catalog = &mut Catalog { columns : Vec::new() };

    for x in 0..TEST_COLS_DENSE_I64 {
        catalog.add_column(BlockType::Int64Dense, format!("col_{}", x));
    }

    let partition = catalog.create_partition();
//    partition.blocks[0];

    println!("Following columns are defined");
    for col in catalog.columns.iter_mut() {
        println!("Column: {} of type {:?}", col.name, col.data_type);
    }

    let elapsed = start.elapsed();
    println!("{:?}", elapsed);
    println!("Elapsed: {} ms",
             (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64);

}
