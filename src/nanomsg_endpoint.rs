use bincode::{serialize, deserialize, Infinite};

use nanomsg::{Socket, Protocol, Error};

use api::{ApiMessage, ApiOperation, scan_and_materialize};
use manager::Manager;

use std::io::{Read, Write};

pub fn start_endpoint(manager : &mut Manager) {
    let mut socket = Socket::new(Protocol::Rep).unwrap();
    let mut endpoint = socket.bind("ipc:///tmp/hyena.ipc").unwrap();

    while true {
        println!("Waiting for message");

        let mut buf: Vec<u8> = Vec::new();
        socket.read_to_end(&mut buf).unwrap();

        println!("Received buffer: {:?}", buf);

        let req : ApiMessage = deserialize(&buf[..]).unwrap();

        match req.op_type {
            ApiOperation::Scan => {
                println!("Scan request: {:?}", req.extract_scan_request());

                let materialized_msg = scan_and_materialize(manager, &req.extract_scan_request());


            },
            ApiOperation::Insert => println!("Insert"),
            _ => println!("Not scan")
        }
    }
}