#![allow(dead_code)]

use std::env::current_dir;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;

const DB_HEADER_SIZE: usize = 100;
const HEADER_STRING_ARR: [u8; 16] = [
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,
];
const HEADER_STR_SZ: (usize, usize) = (0, 16);
const BTREE_HEADER_SIZE: usize = 8;
const INTERIOR_BTREE_HEADER_SIZE: usize = BTREE_HEADER_SIZE + 4;

#[derive(Debug)]
struct InvalidDBFileError {
    details: String,
}

impl InvalidDBFileError {
    fn new() -> Self {
        Self {
            details: "file is not a valid database".to_owned(),
        }
    }
}

impl fmt::Display for InvalidDBFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for InvalidDBFileError {}

pub struct Database {
    pub file: File,
    pub header: [u8; DB_HEADER_SIZE],
}

impl Database {
    pub fn new<P>(db_file: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let mut path = db_file.as_ref().to_path_buf();
        if !path.is_absolute() {
            let cwd = current_dir()?;
            path = cwd.join(path);
        }

        let mut file = File::open(&path).map_err(|e| e.to_string())?;
        let mut header = [0; DB_HEADER_SIZE];
        file.read_exact(&mut header)
            .map_err(|e| e.to_string() + " - database header might be invalid or corrupt")?;

        let header_str_arr: [u8; 16] = header
            [(HEADER_STR_SZ.0)..(HEADER_STR_SZ.0 + HEADER_STR_SZ.1)]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| {
                "error reading header: ".to_owned() + &e.to_string()
            })?;
        validate_db_file(header_str_arr).map_err(|e| e.to_string())?;

        Ok(Self {
            file: file,
            header: header,
        })
    }
}

fn validate_db_file(header_str_arr: [u8; 16]) -> Result<(), InvalidDBFileError> {
    if header_str_arr == HEADER_STRING_ARR {
        Ok(())
    } else {
        Err(InvalidDBFileError::new())
    }
}
