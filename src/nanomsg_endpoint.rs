use bincode::{serialize, deserialize, Infinite};

use nanomsg::{Socket, Protocol, Error};

use api::{ApiMessage, ApiOperation, part_scan_and_materialize, GenericResponse};
use manager::Manager;

use std::io::{Read, Write};
use std::thread;

pub fn start_endpoint(manager : &mut Manager) {
    let mut socket = Socket::new(Protocol::Rep).unwrap();
    let mut endpoint = socket.bind("ipc:///tmp/hyena.ipc").unwrap();

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
            }
            _ => println!("Not supported...")
        }
    }
}