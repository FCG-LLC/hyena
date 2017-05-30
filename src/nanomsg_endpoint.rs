use bincode::{serialize, deserialize, Infinite};

use nanomsg::{Socket, Protocol, Error};
use api::{ApiMessage, ApiOperation};
use std::io::{Read, Write};

pub fn start_endpoint() {
    //let mut socket = try!(Socket::new(Protocol::Rep));
    //let mut endpoint = try!(socket.bind("ipc:///tmp/hyena.ipc"));

    let mut socket = Socket::new(Protocol::Rep).unwrap();
    let mut endpoint = socket.bind("ipc:///tmp/hyena.ipc").unwrap();

    while true {
        println!("Waiting for message");

        let mut buf: Vec<u8> = Vec::new();
        socket.read_to_end(&mut buf).unwrap();

        println!("Received buffer: {:?}", buf);

        let req : ApiMessage = deserialize(&buf[..]).unwrap();

        println!("Deserialized as: {:?}", req.extract_scan_request());

        match req.op_type {
            ApiOperation::Scan => println!("Scan"),
            ApiOperation::Insert => println!("Insert"),
            _ => println!("Not scan")
        }
    }
}