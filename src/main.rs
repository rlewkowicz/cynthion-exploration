#[macro_use]
extern crate bitfield;

mod backend;
mod compact_index;
mod data_stream;
mod id;
mod index_stream;
mod rcu;
mod stream;
mod usb;
mod util;
mod vec_map;
mod capture;
mod decoder;
use hex::encode;

use anyhow::Error;

use crate::backend::cynthion::{
    CynthionDevice,
    CynthionHandle,
    Speed
};

use std::thread::sleep;
use std::time::Duration;

const US: Duration = Duration::from_micros(1);
const MS: Duration = Duration::from_millis(1);

fn main() {
    if let Ok(devices) = CynthionDevice::scan() {
        if let Ok(cynthion) = devices[0].open() {
            if let Ok(cyn) = launch_cythion(cynthion) {
 
            } else {
                println!("notok launch");
            }
        } else {
            println!("notok open");
        }
    } else {
        println!("notok scan");
    }
    
    sleep(Duration::from_secs(1));
}

fn display_error(result: Result<(), Error>) {
    if let Err(e) = result {
       return 
    }
}

fn launch_cythion(cynthion: CynthionHandle) -> Result<(), Error> {

    let handle = std::thread::spawn(move || {
        if let Ok((stream_handle, stop_handle)) = cynthion
        .start(Speed::Full, display_error) {
            for mut packet in stream_handle {
                let hex_string = encode(&packet.bytes);
                // From an xbone controller bytestream I want will always start 4b20 
                if hex_string.starts_with("4b20"){
                    // The right thumbstick is 2 16 bit integers @bytes 15-18
                    let mut first_integer = i16::from_le_bytes([packet.bytes[15], packet.bytes[16]]);
                    let mut second_integer = i16::from_le_bytes([packet.bytes[17], packet.bytes[18]]);
                    println!("- Raw -");
                    println!("First Integer: {}", first_integer);
                    println!("Second Integer: {}", second_integer);
                    println!("Hex short: {}", encode(&packet.bytes[15..19]));
                    println!("Hex full: {}", encode(&packet.bytes));
                    // I would like to take this modification and replay it, or modify entirely inplace
                    first_integer =  32767;
                    second_integer =  3;
                    let first_bytes = first_integer.to_le_bytes();
                    let second_bytes = second_integer.to_le_bytes();
                    packet.bytes[15] = first_bytes[0];
                    packet.bytes[16] = first_bytes[1];
                    packet.bytes[17] = second_bytes[0];
                    packet.bytes[18] = second_bytes[1];
                    println!("- Modified -");
                    println!("First Integer: {}", first_integer);
                    println!("Second Integer: {}", second_integer);
                    println!("Hex short: {}", encode(&packet.bytes[15..19]));
                    println!("Hex full: {}", encode(&packet.bytes));
                }
            }
        } else {
            println!("notok stream");
        }
    });

    handle.join().expect("Thread panicked!");
      
    println!("end of launch");

    Ok(())
}