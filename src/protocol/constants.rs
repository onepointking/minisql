//! MySQL protocol constants

#![allow(dead_code)]

// Capability flags
pub const CLIENT_LONG_PASSWORD: u32 = 1;
pub const CLIENT_FOUND_ROWS: u32 = 2;
pub const CLIENT_LONG_FLAG: u32 = 4;
pub const CLIENT_CONNECT_WITH_DB: u32 = 8;
pub const CLIENT_NO_SCHEMA: u32 = 16;
pub const CLIENT_PROTOCOL_41: u32 = 512;
pub const CLIENT_TRANSACTIONS: u32 = 8192;
pub const CLIENT_SECURE_CONNECTION: u32 = 32768;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

// Command bytes
pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_FIELD_LIST: u8 = 0x04;
pub const COM_CHANGE_USER: u8 = 0x11;
pub const COM_PING: u8 = 0x0E;
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_CLOSE: u8 = 0x19;
pub const COM_STMT_RESET: u8 = 0x1A;
pub const COM_SET_OPTION: u8 = 0x1B;

// Response types
pub const OK_PACKET: u8 = 0x00;
pub const EOF_PACKET: u8 = 0xFE;
pub const ERR_PACKET: u8 = 0xFF;

// Column types for result sets
pub const MYSQL_TYPE_DECIMAL: u8 = 0x00;
pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_SHORT: u8 = 0x02;
pub const MYSQL_TYPE_LONG: u8 = 0x03;
pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_NULL: u8 = 0x06;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_INT24: u8 = 0x09;
pub const MYSQL_TYPE_VARCHAR: u8 = 0x0F;
pub const MYSQL_TYPE_BIT: u8 = 0x10;
pub const MYSQL_TYPE_JSON: u8 = 0xF5;
pub const MYSQL_TYPE_BLOB: u8 = 0xFC;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xFD;
pub const MYSQL_TYPE_STRING: u8 = 0xFE;

// Column flags
pub const NOT_NULL_FLAG: u16 = 0x0001;
pub const PRI_KEY_FLAG: u16 = 0x0002;
pub const UNIQUE_KEY_FLAG: u16 = 0x0004;
pub const MULTIPLE_KEY_FLAG: u16 = 0x0008;
pub const BLOB_FLAG: u16 = 0x0010;
pub const UNSIGNED_FLAG: u16 = 0x0020;
pub const ZEROFILL_FLAG: u16 = 0x0040;
pub const BINARY_FLAG: u16 = 0x0080;
pub const ENUM_FLAG: u16 = 0x0100;
pub const AUTO_INCREMENT_FLAG: u16 = 0x0200;
pub const TIMESTAMP_FLAG: u16 = 0x0400;
pub const SET_FLAG: u16 = 0x0800;
pub const NO_DEFAULT_VALUE_FLAG: u16 = 0x1000;
pub const NUM_FLAG: u16 = 0x8000;
