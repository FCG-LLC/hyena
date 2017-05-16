#[derive(Debug)]
#[allow(unused_variables)]
pub enum ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq
}

pub struct BlockScanConsumer {
    pub matching_offsets : Vec<u32>
}

pub struct ScanMaterializer {

}
