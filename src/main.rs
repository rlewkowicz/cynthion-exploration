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
    
    sleep(Duration::from_secs(5));
}

fn display_error(result: Result<(), Error>) {
    if let Err(e) = result {
       return 
    }
    
}

fn launch_cythion(cynthion: CynthionHandle) -> Result<(), Error> {

   let handle = std::thread::spawn(move || {
            println!("Thread is running!");
            if let Ok((stream_handle, stop_handle)) = cynthion
            .start(Speed::Full, display_error) {
                println!("Thread is running!");

                for packet in stream_handle {
                    let hex_string = encode(&packet.bytes);
                    if hex_string.starts_with("4b20"){
                        println!("Hex string: {}", hex_string);
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