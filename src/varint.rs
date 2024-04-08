#![allow(dead_code)]

use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct MaxBytesExceededError {
    details: &'static str,
}

impl MaxBytesExceededError {
    fn new() -> Self {
        Self {
            details: "Input is invalid for this varint:\n\
                For valid u64 values, the maximum number of bytes must be 9 or less and the last \
                byte must not have a continuation flag (its value must be < 0x80).",
        }
    }
}

impl fmt::Display for MaxBytesExceededError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for MaxBytesExceededError {}

// Encode an unsigned integer up to 64 bits in size to a big-endian varint
pub fn encode_be<T>(value: T) -> (usize, Vec<u8>)
where
    T: Into<u64>,
{
    let value_64bit: u64 = value.into();

    let result: Vec<u8> = (0..64)
        .step_by(7)
        .rev()
        .filter_map(|shift| {
            let byte_value = ((value_64bit >> shift) & 0x7f) as u8;
            if byte_value != 0 || shift == 0 {
                Some(if shift == 0 {
                    byte_value
                } else {
                    byte_value | 0x80
                })
            } else {
                None
            }
        })
        .collect();

    (result.len(), result)
}

// Read a big-endian varint from a slice of bytes
pub fn decode_be(input: &[u8]) -> Result<(u64, usize), MaxBytesExceededError> {
    let mut result = 0u64;
    let mut position = 0;

    for (idx, &byte) in input.iter().enumerate() {
        // If MSB is set, keep accumulating up to max bytes
        if byte > 0x7f {
            if position > 7 {
                return Err(MaxBytesExceededError::new());
            }
            result = (result << 7) | u64::from(byte & 0x7f);
        } else {
            result = (result << 7) | u64::from(byte);
            position = idx;
            break;
        }
    }

    Ok((result, position + 1))
}
