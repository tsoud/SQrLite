#![allow(dead_code)]

use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct MaxBytesExceededError {
    details: String,
}

impl MaxBytesExceededError {
    fn new() -> Self {
        Self {
            details: "Input is invalid for this varint:\n\
                For valid u64 values, the maximum number of bytes must be 9 or less and the last \
                byte must not have a continuation flag (its value must be < 0x80)."
                .to_string(),
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
pub fn encode_be<T>(value: T) -> (Vec<u8>, usize)
where
    T: Into<u64>,
{
    let mut result: Vec<u8> = vec![];
    let value_64bit: u64 = value.into();
    let mut byte_value: u8;

    for shift in (0..63).step_by(7).rev() {
        byte_value = (value_64bit >> shift & 0x7f) as u8;
        if shift != 0 {
            if byte_value == 0 && result.is_empty() {
                continue;
            }
            result.push(byte_value | 0x80);
        } else {
            result.push(byte_value);
        }
    }

    let size = result.len();
    (result, size)
}

// Read a big-endian varint from a slice of bytes
pub fn decode_be(input: &[u8]) -> Result<(u64, usize), MaxBytesExceededError> {
    let mut result = 0u64;
    let mut position = 0;

    for &byte in input.iter() {
        // If MSB is set, keep accumulat
        if byte > 0x7f {
            if position > 7 {
                return Err(MaxBytesExceededError::new());
            }
            result += u64::from(byte) ^ 0x80;
            result <<= 7;
            position += 1;
        } else {
            result += u64::from(byte);
            break;
        }
    }

    Ok((result, position + 1))
}
