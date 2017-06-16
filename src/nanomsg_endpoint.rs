use bincode::{serialize, deserialize, Infinite};

use nanomsg::{Socket, Protocol, Error};

use api::{ApiMessage, ApiOperation, part_scan_and_materialize, GenericResponse, DataCompactionRequest};
use manager::Manager;

use std::io::{Read, Write};
use std::thread;
use std::time::Instant;
use std::fs::{Permissions, metadata, set_permissions};
use std::os::unix::fs::PermissionsExt;

const FLUSH_AFTER_ROWS: usize = 10_000;
const FLUSH_AFTER_SECS: usize = 300;
const HYENA_SOCKET_PATH: &str = "/tmp/hyena.ipc";

pub fn start_endpoint(manager : &mut Manager) {
    let mut socket = Socket::new(Protocol::Rep).unwrap();
    let mut endpoint = socket.bind(&format!("ipc://{}", HYENA_SOCKET_PATH)).unwrap();
    let mut perms = metadata(HYENA_SOCKET_PATH).unwrap().permissions();
    perms.set_mode(0o775);
    set_permissions(HYENA_SOCKET_PATH, perms).unwrap();
    let mut last_flush = None::<Instant>;
    let mut rows_inserted = 0_usize;

    while true {
        println!("Waiting for message...");

        let mut buf: Vec<u8> = Vec::new();
        socket.read_to_end(&mut buf).unwrap();

//        println!("Received buffer: {:?}", buf);

        let req : ApiMessage = deserialize(&buf[..]).unwrap();

        match req.op_type {
            ApiOperation::Scan => {
                println!("Scan request: {:?}", req.extract_scan_request());

                let scan_request = req.extract_scan_request();
                let materialized_msg = part_scan_and_materialize(manager, &scan_request);
                let buf = serialize(&materialized_msg, Infinite).unwrap();
                socket.write(&buf).unwrap();
            },
            ApiOperation::RefreshCatalog => {
                println!("Refresh catalog response");

                let buf = serialize(&manager.catalog, Infinite).unwrap();
                socket.write(&buf).unwrap();
            }
            ApiOperation::Insert => {
                println!("Insert request");
                let materialized_msg = &req.extract_insert_message();
                manager.insert(&materialized_msg);

                rows_inserted += materialized_msg.row_count as usize;

                socket.write(&GenericResponse::create_as_buf(0));
            },
            ApiOperation::AddColumn => {
                println!("Add column request");
                let materialized_msg = &req.extract_add_column_message();
                manager.add_column(materialized_msg.column_type.to_owned(), materialized_msg.column_name.to_owned());

                socket.write(&GenericResponse::create_as_buf(0));
            },
            ApiOperation::Flush => {
                println!("Flush request");
                manager.dump_in_mem_partition();

                socket.write(&GenericResponse::create_as_buf(0));
            },
            ApiOperation::DataCompaction => {
                println!("Data compaction");

                let compaction_msg = &req.extract_data_compaction_request();
                // TODO: fun
                
                println!("DATA COMPACTION NOT IMPLEMENTED");

                socket.write(&GenericResponse::create_as_buf(0));
            }
            _ => println!("Not supported...")
        }

        // check if we need to flush
        if rows_inserted > FLUSH_AFTER_ROWS || if let Some(last_flush) = last_flush {
            last_flush.elapsed().as_secs() > FLUSH_AFTER_SECS as u64
        } else { false } {
            last_flush = Some(Instant::now());
            rows_inserted = 0;
            println!("Forced flush");
            manager.dump_in_mem_partition();
        }
    }
}