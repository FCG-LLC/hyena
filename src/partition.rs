use scan::ScanComparison;
use scan::BlockScanConsumer;
use int_blocks::Int64DenseBlock;

pub struct Partition {
    pub blocks : Vec<Int64DenseBlock>
}
