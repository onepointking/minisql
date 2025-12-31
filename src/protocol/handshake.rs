//! MySQL authentication and handshake handling

use rand::Rng;
use sha1::{Digest, Sha1};

use crate::error::{MiniSqlError, Result};
use crate::Config;

use super::constants::*;
use super::packet::{LenencInt, PacketIO};

/// Handles MySQL handshake and authentication
pub struct HandshakeHandler {
    auth_data: [u8; 20],
}

impl HandshakeHandler {
    /// Create a new handshake handler with random auth challenge
    pub fn new() -> Self {
        let mut auth_data = [0u8; 20];
        rand::thread_rng().fill(&mut auth_data);
        Self { auth_data }
    }

    /// Send the initial handshake packet
    pub async fn send_handshake(&self, io: &mut PacketIO) -> Result<()> {
        let mut packet = Vec::new();

        // Protocol version (10)
        packet.push(10);

        // Server version (null-terminated)
        packet.extend_from_slice(b"5.7.0-MiniSQL\0");

        // Connection ID (4 bytes, little-endian)
        packet.extend_from_slice(&1u32.to_le_bytes());

        // Auth-plugin-data-part-1 (8 bytes)
        packet.extend_from_slice(&self.auth_data[0..8]);

        // Filler (1 byte)
        packet.push(0);

        // Capability flags (lower 2 bytes)
        let capabilities: u32 = CLIENT_LONG_PASSWORD
            | CLIENT_FOUND_ROWS
            | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        packet.extend_from_slice(&(capabilities as u16).to_le_bytes());

        // Character set (utf8mb4 = 45)
        packet.push(45);

        // Status flags (2 bytes)
        packet.extend_from_slice(&0u16.to_le_bytes());

        // Capability flags (upper 2 bytes)
        packet.extend_from_slice(&((capabilities >> 16) as u16).to_le_bytes());

        // Length of auth-plugin-data (1 byte) - 21 for mysql_native_password
        packet.push(21);

        // Reserved (10 bytes of zeros)
        packet.extend_from_slice(&[0u8; 10]);

        // Auth-plugin-data-part-2 (12 bytes + 1 null terminator)
        packet.extend_from_slice(&self.auth_data[8..20]);
        packet.push(0);

        // Auth plugin name (null-terminated)
        packet.extend_from_slice(b"mysql_native_password\0");

        io.reset_sequence();
        io.write_packet(&packet).await?;

        Ok(())
    }

    /// Return a copy of the handshake auth challenge bytes used in the initial handshake
    pub fn auth_data(&self) -> [u8; 20] {
        self.auth_data
    }

    /// Parse the client's handshake response and validate credentials
    /// Returns the client capabilities on success
    pub fn parse_and_validate(&self, data: &[u8], config: &Config) -> Result<u32> {
        if data.len() < 32 {
            return Err(MiniSqlError::Protocol("Handshake response too short".into()));
        }

        let mut pos = 0;

        // Client capabilities (4 bytes)
        let capabilities = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        pos += 4;

        // Max packet size (4 bytes) - skip
        pos += 4;

        // Character set (1 byte) - skip
        pos += 1;

        // Reserved (23 bytes) - skip
        pos += 23;

        // Username (null-terminated)
        let username_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| MiniSqlError::Protocol("Invalid username".into()))?;
        let username = String::from_utf8_lossy(&data[pos..pos + username_end]).to_string();
        pos += username_end + 1;

        // Auth response
        let auth_response = if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
            // Length-encoded auth data
            let (len, bytes_read) = LenencInt::read(&data[pos..])?;
            pos += bytes_read;
            let auth = &data[pos..pos + len as usize];
            // no need to advance `pos` here; it's not read after this branch
            auth.to_vec()
        } else if capabilities & CLIENT_SECURE_CONNECTION != 0 {
            // 1-byte length prefix
            let len = data[pos] as usize;
            pos += 1;
            let auth = &data[pos..pos + len];
            // no need to advance `pos` here; it's not read after this branch
            auth.to_vec()
        } else {
            // Null-terminated
            let end = data[pos..].iter().position(|&b| b == 0).unwrap_or(0);
            let auth = &data[pos..pos + end];
            // pos += end + 1;  // Not needed as we don't use pos after this
            auth.to_vec()
        };

        // Validate credentials
        if username != config.username {
            return Err(MiniSqlError::Auth(format!(
                "Access denied for user '{}'",
                username
            )));
        }

        // Verify password if auth_response is not empty
        if !auth_response.is_empty() && !config.password.is_empty() {
            let expected = compute_auth_response(&config.password, &self.auth_data);
            if auth_response != expected {
                return Err(MiniSqlError::Auth(format!(
                    "Access denied for user '{}' (using password: YES)",
                    username
                )));
            }
        }

        Ok(capabilities)
    }
}

/// Parse a COM_CHANGE_USER packet payload and validate credentials.
///
/// The COM_CHANGE_USER payload contains: <user NUL><auth_response>...<database NUL>...[optional fields]
/// This function uses the provided `client_capabilities` to know how the auth_response is encoded
/// and the `challenge` (from the original handshake) to validate mysql_native_password responses.
pub fn parse_and_validate_change_user(
    data: &[u8],
    client_capabilities: u32,
    challenge: &[u8; 20],
    config: &Config,
) -> Result<u32> {
    let mut pos = 0usize;

    // username (NUL-terminated)
    let username_end = data[pos..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| MiniSqlError::Protocol("Invalid username in COM_CHANGE_USER".into()))?;
    let username = String::from_utf8_lossy(&data[pos..pos + username_end]).to_string();
    pos += username_end + 1;

    // auth response
    let auth_response = if client_capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        // length-encoded
        let (len, read) = LenencInt::read(&data[pos..])?;
        pos += read;
        if pos + len as usize > data.len() {
            return Err(MiniSqlError::Protocol("Truncated auth response".into()));
        }
        let auth = &data[pos..pos + len as usize];
        pos += len as usize;
        auth.to_vec()
    } else if client_capabilities & CLIENT_SECURE_CONNECTION != 0 {
        // 1-byte length prefix
        if pos >= data.len() {
            return Err(MiniSqlError::Protocol("Truncated auth response length".into()));
        }
        let len = data[pos] as usize;
        pos += 1;
        if pos + len > data.len() {
            return Err(MiniSqlError::Protocol("Truncated auth response".into()));
        }
        let auth = &data[pos..pos + len];
        pos += len;
        auth.to_vec()
    } else {
        // NUL-terminated
        let end = data[pos..].iter().position(|&b| b == 0).unwrap_or(0);
        let auth = &data[pos..pos + end];
        pos += end + 1;
        auth.to_vec()
    };

    // database (NUL-terminated) - may be empty string
    let _db_name = if pos < data.len() {
        let db_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| MiniSqlError::Protocol("Invalid database in COM_CHANGE_USER".into()))?;
    let db = String::from_utf8_lossy(&data[pos..pos + db_end]).to_string();
    // advancing `pos` here is unnecessary because it's not used after parsing the DB name
        db
    } else {
        String::new()
    };

    // Validate username
    if username != config.username {
        return Err(MiniSqlError::Auth(format!(
            "Access denied for user '{}'",
            username
        )));
    }

    // If auth response present and server has password configured, validate
    if !auth_response.is_empty() && !config.password.is_empty() {
        let expected = compute_auth_response(&config.password, challenge);
        if auth_response != expected {
            return Err(MiniSqlError::Auth(format!(
                "Access denied for user '{}' (using password: YES)",
                username
            )));
        }
    }

    // For now, we don't process optional trailing fields (charset, plugin name, attrs)

    Ok(client_capabilities)
}

/// Compute the expected auth response for mysql_native_password
/// SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password)))
fn compute_auth_response(password: &str, challenge: &[u8]) -> Vec<u8> {
    // SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // SHA1(challenge + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(challenge);
    hasher.update(&stage2);
    let stage3 = hasher.finalize();

    // XOR SHA1(password) with result
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_auth_response() {
        let password = "test_password";
        let challenge = b"12345678901234567890";
        
        let response = compute_auth_response(password, challenge);
        
        // Should produce 20 bytes (SHA1 output size)
        assert_eq!(response.len(), 20);
        
        // Same inputs should produce same output
        let response2 = compute_auth_response(password, challenge);
        assert_eq!(response, response2);
    }

    #[test]
    fn test_compute_auth_response_different_passwords() {
        let challenge = b"12345678901234567890";
        
        let response1 = compute_auth_response("password1", challenge);
        let response2 = compute_auth_response("password2", challenge);
        
        // Different passwords should produce different responses
        assert_ne!(response1, response2);
    }

    #[test]
    fn test_compute_auth_response_different_challenges() {
        let password = "test_password";
        
        let response1 = compute_auth_response(password, b"12345678901234567890");
        let response2 = compute_auth_response(password, b"09876543210987654321");
        
        // Different challenges should produce different responses
        assert_ne!(response1, response2);
    }

    #[test]
    fn test_handshake_handler_creation() {
        let handler1 = HandshakeHandler::new();
        let handler2 = HandshakeHandler::new();
        
        // Each handler should have different random auth data
        assert_ne!(handler1.auth_data, handler2.auth_data);
    }

    #[test]
    fn test_parse_handshake_response_too_short() {
        let handler = HandshakeHandler::new();
        let config = Config::default();
        let short_data = vec![0u8; 10];
        
        let result = handler.parse_and_validate(&short_data, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_handshake_response_wrong_username() {
        let handler = HandshakeHandler::new();
        let mut config = Config::default();
        config.username = "correct_user".to_string();
        
        // Build minimal valid handshake response with wrong username
        let mut data = vec![0u8; 32];
        // Capabilities
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        // Username at position 32
        data.extend_from_slice(b"wrong_user\0");
        // Auth response (empty)
        data.push(0);
        
        let result = handler.parse_and_validate(&data, &config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Access denied"));
        }
    }
}
