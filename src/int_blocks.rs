use scan::ScanComparison;
use scan::BlockScanConsumer;

pub struct Int64DenseBlock {
    pub data : Vec<u64>
}

impl Int64DenseBlock {
    pub fn append(&mut self, v : u64) {
        self.data.push(v);
    }

    pub fn scan(&self, op : &ScanComparison, val : &u64, scan_consumer : &mut BlockScanConsumer) {
        for (offset_usize, value) in self.data.iter().enumerate() {
            let offset = offset_usize as u32;
            match op {
                Lt => if value < val { scan_consumer.matching_offsets.push(offset) },
                LtEq => if value <= val { scan_consumer.matching_offsets.push(offset) },
                Eq => if value == val { scan_consumer.matching_offsets.push(offset) },
                GtEq => if value >= val { scan_consumer.matching_offsets.push(offset) },
                Gt => if value > val { scan_consumer.matching_offsets.push(offset) },
                NotEq => if value != val { scan_consumer.matching_offsets.push(offset) },
            }
        }
    }
}