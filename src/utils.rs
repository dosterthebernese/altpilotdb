//use core::error::Error;

use std::error::Error;
use data_encoding::HEXUPPER;
use ring::digest::{Context, Digest, SHA256};
use std::fs::File;
use std::io::{BufReader, Read};


pub fn sha256_digest<R: Read>(mut reader: R) -> Result<Digest, Box<dyn Error>> {
    let mut context = Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(context.finish())
}

pub fn sha_fmt(ifile: &str) -> Result<String, Box<dyn Error>> {

    let input = File::open(ifile).unwrap();
    let reader = BufReader::new(input);

    let digest = sha256_digest(reader)?;
    Ok(HEXUPPER.encode(digest.as_ref()))
}


#[cfg(test)]
mod tests {
}


