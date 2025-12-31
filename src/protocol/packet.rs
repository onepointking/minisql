//! Low-level MySQL packet I/O and encoding utilities
//!
//! Handles reading/writing MySQL protocol packets and length-encoded values.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{MiniSqlError, Result};

/// Handles low-level packet I/O operations for MySQL protocol
pub struct PacketIO {
    stream: TcpStream,
    sequence_id: u8,
}

impl PacketIO {
    /// Create a new PacketIO instance
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            sequence_id: 0,
        }
    }

    /// Get current sequence ID
    #[allow(dead_code)]
    pub fn sequence_id(&self) -> u8 {
        self.sequence_id
    }

    /// Set sequence ID (used after handshake)
    #[allow(dead_code)]
    pub fn set_sequence_id(&mut self, id: u8) {
        self.sequence_id = id;
    }

    /// Reset sequence ID to 0
    pub fn reset_sequence(&mut self) {
        self.sequence_id = 0;
    }

    /// Read a packet from the client
    /// Returns the payload bytes
    pub async fn read_packet(&mut self) -> Result<Vec<u8>> {
        // Read 4-byte header
        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await?;

        // Parse header
        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        // Store the client's sequence ID and increment for our next response
        self.sequence_id = header[3].wrapping_add(1);

        // Read payload
        let mut payload = vec![0u8; payload_len];
        self.stream.read_exact(&mut payload).await?;

        Ok(payload)
    }

    /// Write a packet to the client
    pub async fn write_packet(&mut self, payload: &[u8]) -> Result<()> {
        let len = payload.len() as u32;
        let header = [
            (len & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            ((len >> 16) & 0xFF) as u8,
            self.sequence_id,
        ];

        self.stream.write_all(&header).await?;
        self.stream.write_all(payload).await?;
        self.stream.flush().await?;

        self.sequence_id = self.sequence_id.wrapping_add(1);
        Ok(())
    }

    /// Consume self and return the underlying stream
    #[allow(dead_code)]
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

/// Length-encoded integer encoding/decoding utilities
pub struct LenencInt;

impl LenencInt {
    /// Write a length-encoded integer to a buffer
    pub fn write(buf: &mut Vec<u8>, val: u64) {
        if val < 251 {
            buf.push(val as u8);
        } else if val < 65536 {
            buf.push(0xFC);
            buf.extend_from_slice(&(val as u16).to_le_bytes());
        } else if val < 16777216 {
            buf.push(0xFD);
            buf.push((val & 0xFF) as u8);
            buf.push(((val >> 8) & 0xFF) as u8);
            buf.push(((val >> 16) & 0xFF) as u8);
        } else {
            buf.push(0xFE);
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    /// Read a length-encoded integer, returning (value, bytes_read)
    pub fn read(data: &[u8]) -> Result<(u64, usize)> {
        if data.is_empty() {
            return Err(MiniSqlError::Protocol("Empty lenenc int".into()));
        }

        let first = data[0];
        if first < 251 {
            Ok((first as u64, 1))
        } else if first == 0xFC {
            if data.len() < 3 {
                return Err(MiniSqlError::Protocol("Truncated lenenc int".into()));
            }
            let val = u16::from_le_bytes([data[1], data[2]]) as u64;
            Ok((val, 3))
        } else if first == 0xFD {
            if data.len() < 4 {
                return Err(MiniSqlError::Protocol("Truncated lenenc int".into()));
            }
            let val = u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64;
            Ok((val, 4))
        } else if first == 0xFE {
            if data.len() < 9 {
                return Err(MiniSqlError::Protocol("Truncated lenenc int".into()));
            }
            let val = u64::from_le_bytes([
                data[1], data[2], data[3], data[4],
                data[5], data[6], data[7], data[8],
            ]);
            Ok((val, 9))
        } else {
            Err(MiniSqlError::Protocol(format!("Invalid lenenc int prefix: {}", first)))
        }
    }
}

/// Length-encoded string utilities
pub struct LenencString;

impl LenencString {
    /// Write a length-encoded string to a buffer
    pub fn write(buf: &mut Vec<u8>, s: &str) {
        LenencInt::write(buf, s.len() as u64);
        buf.extend_from_slice(s.as_bytes());
    }

    /// Read a length-encoded string, returning (string, bytes_read)
    #[allow(dead_code)]
    pub fn read(data: &[u8]) -> Result<(String, usize)> {
        let (len, bytes_read) = LenencInt::read(data)?;
        let start = bytes_read;
        let end = start + len as usize;
        
        if data.len() < end {
            return Err(MiniSqlError::Protocol("Truncated string value".into()));
        }
        
        let s = String::from_utf8_lossy(&data[start..end]).to_string();
        Ok((s, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lenenc_int_small() {
        let mut buf = Vec::new();
        LenencInt::write(&mut buf, 42);
        assert_eq!(buf, vec![42]);

        let (val, len) = LenencInt::read(&buf).unwrap();
        assert_eq!(val, 42);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_lenenc_int_medium() {
        let mut buf = Vec::new();
        LenencInt::write(&mut buf, 1000);
        assert_eq!(buf, vec![0xFC, 0xE8, 0x03]);

        let (val, len) = LenencInt::read(&buf).unwrap();
        assert_eq!(val, 1000);
        assert_eq!(len, 3);
    }

    #[test]
    fn test_lenenc_int_large() {
        let mut buf = Vec::new();
        LenencInt::write(&mut buf, 16777216);
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[0], 0xFE);

        let (val, len) = LenencInt::read(&buf).unwrap();
        assert_eq!(val, 16777216);
        assert_eq!(len, 9);
    }

    #[test]
    fn test_lenenc_int_boundary_250() {
        let mut buf = Vec::new();
        LenencInt::write(&mut buf, 250);
        assert_eq!(buf, vec![250]);

        let (val, len) = LenencInt::read(&buf).unwrap();
        assert_eq!(val, 250);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_lenenc_int_boundary_251() {
        let mut buf = Vec::new();
        LenencInt::write(&mut buf, 251);
        assert_eq!(buf, vec![0xFC, 0xFB, 0x00]);

        let (val, len) = LenencInt::read(&buf).unwrap();
        assert_eq!(val, 251);
        assert_eq!(len, 3);
    }

    #[test]
    fn test_lenenc_string() {
        let mut buf = Vec::new();
        LenencString::write(&mut buf, "hello");
        assert_eq!(buf, vec![5, b'h', b'e', b'l', b'l', b'o']);

        let (s, len) = LenencString::read(&buf).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(len, 6);
    }

    #[test]
    fn test_lenenc_string_empty() {
        let mut buf = Vec::new();
        LenencString::write(&mut buf, "");
        assert_eq!(buf, vec![0]);

        let (s, len) = LenencString::read(&buf).unwrap();
        assert_eq!(s, "");
        assert_eq!(len, 1);
    }

    #[test]
    fn test_lenenc_string_long() {
        let long_string = "a".repeat(300);
        let mut buf = Vec::new();
        LenencString::write(&mut buf, &long_string);

        let (s, _) = LenencString::read(&buf).unwrap();
        assert_eq!(s, long_string);
    }

    #[test]
    fn test_lenenc_int_truncated() {
        let buf = vec![0xFC, 0x00]; // Missing one byte
        assert!(LenencInt::read(&buf).is_err());
    }

    #[test]
    fn test_lenenc_string_truncated() {
        let buf = vec![5, b'h', b'i']; // Says 5 bytes but only has 2
        assert!(LenencString::read(&buf).is_err());
    }
}
