use catalog::PartitionInfo;
use api::{ScanResultMessage, ScanFilter, ScanComparison};
use catalog::Catalog;
use manager::{Manager, BlockCache};



#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BlockScanConsumer {
    pub matching_offsets : Vec<u32>
}


impl BlockScanConsumer {
    pub fn merge_or_scans(scans : &Vec<BlockScanConsumer>) -> BlockScanConsumer {
        let mut new_matching_offsets : Vec<u32> = Vec::new();

        let mut offset_sets:Vec<&Vec<u32>> = Vec::new();

        for scan in scans {
            offset_sets.push(&scan.matching_offsets);
        }

        let mut iterators:Vec<usize> = vec![0; offset_sets.len()];

        let mut min_offset:u32 = 0;

        let mut closed_iterators_count = 0;

        while closed_iterators_count < iterators.len()  {
            // Find the smallest having value equal to cur_min_offset or greater
            closed_iterators_count = 0;

            let mut best_consumer_index = 0;
            let mut best_offset = u32::max_value();

            for consumer_index in 0..iterators.len() {
                // Make sure the lagging ones are moved forward
                while iterators[consumer_index] < offset_sets[consumer_index].len() && offset_sets[consumer_index][iterators[consumer_index]] < min_offset {
                    iterators[consumer_index] += 1;
                }

                let consumer_position = iterators[consumer_index];
                if consumer_position < offset_sets[consumer_index].len() {
                    // Make sure the lagging ones are moved forward
                    let potential_offset = offset_sets[consumer_index][consumer_position];

                    if potential_offset < best_offset {
                        best_offset = potential_offset;
                        best_consumer_index = consumer_index;
                    }
                } else {
                    closed_iterators_count += 1;
                }
            }

            if best_offset >= min_offset && best_offset != u32::max_value() {
                new_matching_offsets.push(best_offset);
                iterators[best_consumer_index] += 1;

                min_offset = best_offset + 1; // we are looking for next minimum value
            } else {
//                fail!("Ooopsie");
            }

            if iterators[best_consumer_index] == offset_sets[best_consumer_index].len() {
                closed_iterators_count += 1;
            }
        }

        BlockScanConsumer { matching_offsets: new_matching_offsets }
    }

    pub fn merge_and_scans(scans : &Vec<BlockScanConsumer>) -> BlockScanConsumer {
        let mut new_matching_offsets : Vec<u32> = Vec::new();

        let mut offset_sets:Vec<&Vec<u32>> = Vec::new();

        for scan in scans {
            offset_sets.push(&scan.matching_offsets);
        }

        let mut iterators:Vec<usize> = vec![0; offset_sets.len()];

        let mut min_offset:u32 = 0;

        let mut closed_iterators_count = 0;

        while closed_iterators_count == 0 {
            for consumer_index in 0..iterators.len() {
                if iterators[consumer_index] >= offset_sets[consumer_index].len() {
                    closed_iterators_count += 1;
                }
            }

            if closed_iterators_count > 0 { break; }

            // Get the largest offset in current iteration
            //let max_offset_value = iterators.iter().enumerate().map(|(consumer_index, consumer_position)| offset_sets[consumer_index][consumer_position]).max();
            let max_offset_value = iterators.iter().enumerate().map(|(consumer_index, consumer_position)| offset_sets[consumer_index][*consumer_position]).max().unwrap();

            // Iterate all others until they match the max (or to next item)
            let mut matching_count = 0;
            for consumer_index in 0..iterators.len() {
                while iterators[consumer_index] < offset_sets[consumer_index].len() && offset_sets[consumer_index][iterators[consumer_index]] < max_offset_value {
                    iterators[consumer_index] += 1;
                }
                if iterators[consumer_index] < offset_sets[consumer_index].len() && offset_sets[consumer_index][iterators[consumer_index]] == max_offset_value {
                    matching_count += 1;
                }
            }

            if matching_count == iterators.len() {
                new_matching_offsets.push(max_offset_value);
                // increment all iterators
                for consumer_index in 0..iterators.len() {
                    iterators[consumer_index] += 1;
                }
            }
        }

        BlockScanConsumer { matching_offsets: new_matching_offsets }
    }

    pub fn materialize(&self, manager : &Manager, block_cache: &mut BlockCache, projection : &Vec<u32>, msg : &mut ScanResultMessage) {
        // This should work only on empty message (different implementation is of course possible,
        // if you think it would make sense to merge results)
        assert_eq!(msg.row_count, 0);

        msg.row_count = self.matching_offsets.len() as u32;
        msg.col_count = projection.len() as u32;

        for col_index in projection {
            let column = &manager.catalog.columns[*col_index as usize];
            msg.col_types.push((*col_index, column.data_type.to_owned()));

            // Fetch block from disk
            let block_maybe = block_cache.cached_block_maybe(*col_index);
            match block_maybe {
                None => {
                    let block = manager.load_block(&block_cache.partition_info, *col_index);
                    msg.blocks.push(block.consume(self));
                },
                Some(ref x) => {
                    msg.blocks.push(x.consume(self));
                }
            };

        }
    }

}


#[test]
fn it_merges_consumers() {
    let consumers = vec![
        BlockScanConsumer {
            matching_offsets: vec![101,102,103,510,512,514]
        },
        BlockScanConsumer{
            matching_offsets: vec![0,1,101,514,515]
        },
        BlockScanConsumer{
            matching_offsets: vec![513]
        },
        BlockScanConsumer{
            matching_offsets: vec![]
        }
    ];

    assert_eq!(
        BlockScanConsumer{
            matching_offsets: vec![0,1,101,102,103,510,512,513,514,515]
        },
        BlockScanConsumer::merge_or_scans(&consumers)
    );

    assert_eq!(
        BlockScanConsumer{
            matching_offsets: vec![]
        },
        BlockScanConsumer::merge_and_scans(&consumers)
    );

    let consumers2 = vec![
        BlockScanConsumer {
            matching_offsets: vec![101,102,103,510,512,514]
        },
        BlockScanConsumer{
            matching_offsets: vec![0,1,101,102,514,515]
        },
        BlockScanConsumer{
            matching_offsets: vec![101,102,514]
        }
    ];

    assert_eq!(
        BlockScanConsumer{
            matching_offsets: vec![101,102,514]
        },
        BlockScanConsumer::merge_and_scans(&consumers2)
    );

}