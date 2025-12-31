#!/usr/bin/env python3
"""
Capture and decode MySQL protocol packets
This helps us see exactly what bytes are being sent over the wire
"""

import socket
import struct
import sys
from datetime import datetime

def read_packet(sock):
    """Read a MySQL packet (4-byte header + payload)"""
    try:
        header = sock.recv(4)
        if len(header) < 4:
            return None, None, None
            
        payload_length = struct.unpack('<I', header[:3] + b'\x00')[0]
        sequence_id = header[3]
        
        payload = b''
        while len(payload) < payload_length:
            chunk = sock.recv(payload_length - len(payload))
            if not chunk:
                break
            payload += chunk
            
        return sequence_id, payload, header + payload
    except:
        return None, None, None

def decode_lenenc_int(data, offset=0):
    """Decode MySQL length-encoded integer"""
    if offset >= len(data):
        return None, offset
        
    first_byte = data[offset]
    
    if first_byte < 0xFB:
        return first_byte, offset + 1
    elif first_byte == 0xFC:
        if offset + 3 > len(data):
            return None, offset
        return struct.unpack('<H', data[offset+1:offset+3])[0], offset + 3
    elif first_byte == 0xFD:
        if offset + 4 > len(data):
            return None, offset
        return struct.unpack('<I', data[offset+1:offset+4] + b'\x00')[0], offset + 4
    elif first_byte == 0xFE:
        if offset + 9 > len(data):
            return None, offset
        return struct.unpack('<Q', data[offset+1:offset+9])[0], offset + 9
    else:
        return None, offset

def decode_lenenc_string(data, offset=0):
    """Decode MySQL length-encoded string"""
    length, new_offset = decode_lenenc_int(data, offset)
    if length is None:
        return None, offset
    
    if new_offset + length > len(data):
        return None, offset
        
    string = data[new_offset:new_offset+length]
    return string, new_offset + length

def hex_dump(data, prefix="  "):
    """Pretty print hex dump"""
    for i in range(0, len(data), 16):
        chunk = data[i:i+16]
        hex_str = ' '.join(f'{b:02x}' for b in chunk)
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        print(f"{prefix}{i:04x}  {hex_str:<48}  {ascii_str}")

def decode_column_definition(payload):
    """Decode a column definition packet"""
    offset = 0
    
    # Catalog
    catalog, offset = decode_lenenc_string(payload, offset)
    # Schema
    schema, offset = decode_lenenc_string(payload, offset)
    # Table
    table, offset = decode_lenenc_string(payload, offset)
    # Org table
    org_table, offset = decode_lenenc_string(payload, offset)
    # Name
    name, offset = decode_lenenc_string(payload, offset)
    # Org name
    org_name, offset = decode_lenenc_string(payload, offset)
    
    # Fixed length fields marker (0x0C)
    if offset >= len(payload) or payload[offset] != 0x0C:
        return None
    offset += 1
    
    # Character set (2 bytes)
    charset = struct.unpack('<H', payload[offset:offset+2])[0]
    offset += 2
    
    # Column length (4 bytes)
    col_length = struct.unpack('<I', payload[offset:offset+4])[0]
    offset += 4
    
    # Type (1 byte)
    col_type = payload[offset]
    offset += 1
    
    # Flags (2 bytes)
    flags = struct.unpack('<H', payload[offset:offset+2])[0]
    offset += 2
    
    # Decimals (1 byte)
    decimals = payload[offset]
    offset += 1
    
    type_names = {
        0: 'DECIMAL', 1: 'TINY', 2: 'SHORT', 3: 'LONG', 4: 'FLOAT',
        5: 'DOUBLE', 6: 'NULL', 7: 'TIMESTAMP', 8: 'LONGLONG', 9: 'INT24',
        10: 'DATE', 11: 'TIME', 12: 'DATETIME', 13: 'YEAR', 15: 'VARCHAR',
        16: 'BIT', 245: 'JSON', 246: 'DECIMAL', 247: 'ENUM', 248: 'SET',
        249: 'TINY_BLOB', 250: 'MEDIUM_BLOB', 251: 'LONG_BLOB', 252: 'BLOB',
        253: 'VAR_STRING', 254: 'STRING'
    }
    
    flag_names = []
    if flags & 0x0001: flag_names.append('NOT_NULL')
    if flags & 0x0002: flag_names.append('PRI_KEY')
    if flags & 0x0020: flag_names.append('UNSIGNED')
    if flags & 0x0080: flag_names.append('BINARY')
    if flags & 0x0200: flag_names.append('AUTO_INCREMENT')
    if flags & 0x8000: flag_names.append('NUM')
    
    return {
        'name': name.decode('utf-8', errors='ignore'),
        'type': type_names.get(col_type, f'UNKNOWN({col_type})'),
        'type_code': col_type,
        'charset': charset,
        'length': col_length,
        'flags': flags,
        'flag_names': flag_names,
        'decimals': decimals
    }

def decode_binary_row(payload, col_count, col_types):
    """Decode a binary result row"""
    if payload[0] != 0x00:
        return None
    
    offset = 1
    
    # NULL bitmap
    null_bitmap_len = (col_count + 7 + 2) // 8
    null_bitmap = payload[offset:offset+null_bitmap_len]
    offset += null_bitmap_len
    
    values = []
    for i, col_type in enumerate(col_types):
        # Check if NULL
        bit_pos = i + 2
        if null_bitmap[bit_pos // 8] & (1 << (bit_pos % 8)):
            values.append(('NULL', None))
            continue
        
        # Decode based on type
        if col_type == 8:  # LONGLONG
            val = struct.unpack('<q', payload[offset:offset+8])[0]
            offset += 8
            values.append(('LONGLONG', val))
        elif col_type == 5:  # DOUBLE
            val = struct.unpack('<d', payload[offset:offset+8])[0]
            offset += 8
            values.append(('DOUBLE', val))
        elif col_type == 1:  # TINY
            val = payload[offset]
            offset += 1
            values.append(('TINY', val))
        elif col_type in [253, 254]:  # VAR_STRING, STRING
            str_val, offset = decode_lenenc_string(payload, offset)
            values.append(('STRING', str_val.decode('utf-8', errors='ignore') if str_val else ''))
        elif col_type == 252:  # BLOB/TEXT
            str_val, offset = decode_lenenc_string(payload, offset)
            values.append(('TEXT', str_val.decode('utf-8', errors='ignore') if str_val else ''))
        else:
            values.append(('UNKNOWN', f'type_{col_type}'))
            break
    
    return values

def decode_text_row(payload, col_count):
    """Decode a text result row"""
    values = []
    offset = 0
    
    for i in range(col_count):
        if offset >= len(payload):
            break
            
        if payload[offset] == 0xFB:  # NULL
            values.append(('NULL', None))
            offset += 1
        else:
            str_val, offset = decode_lenenc_string(payload, offset)
            if str_val is not None:
                values.append(('STRING', str_val.decode('utf-8', errors='ignore')))
            else:
                break
    
    return values

def proxy_connection():
    """Act as a proxy between client and server, capturing packets"""
    # Listen for client connections
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('127.0.0.1', 3307))
    server_sock.listen(1)
    
    print("Proxy listening on 127.0.0.1:3307")
    print("Forwarding to 127.0.0.1:3306")
    print("=" * 80)
    print()
    
    client_sock, client_addr = server_sock.accept()
    print(f"Client connected from {client_addr}")
    
    # Connect to actual MySQL server
    mysql_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mysql_sock.connect(('127.0.0.1', 3306))
    print("Connected to MySQL server")
    print()
    
    packet_num = 0
    col_definitions = []
    col_count = 0
    in_result_set = False
    is_binary_protocol = False
    
    import select
    
    while True:
        readable, _, _ = select.select([client_sock, mysql_sock], [], [], 1.0)
        
        for sock in readable:
            if sock == client_sock:
                # Client -> Server
                seq_id, payload, full_packet = read_packet(sock)
                if payload is None:
                    print("Client disconnected")
                    return
                
                packet_num += 1
                print(f"\n{'='*80}")
                print(f"[Packet #{packet_num}] CLIENT -> SERVER")
                print(f"Time: {datetime.now().strftime('%H:%M:%S.%f')}")
                print(f"Sequence ID: {seq_id}, Length: {len(payload)}")
                
                # Check for COM_STMT_EXECUTE (binary protocol)
                if len(payload) > 0 and payload[0] == 0x17:
                    is_binary_protocol = True
                    print("Command: COM_STMT_EXECUTE (Binary Protocol)")
                elif len(payload) > 0 and payload[0] == 0x03:
                    is_binary_protocol = False
                    query = payload[1:].decode('utf-8', errors='ignore')
                    print(f"Command: COM_QUERY (Text Protocol)")
                    print(f"Query: {query}")
                
                hex_dump(payload)
                
                # Forward to server
                mysql_sock.sendall(full_packet)
                
            else:
                # Server -> Client
                seq_id, payload, full_packet = read_packet(sock)
                if payload is None:
                    print("Server disconnected")
                    return
                
                packet_num += 1
                print(f"\n{'='*80}")
                print(f"[Packet #{packet_num}] SERVER -> CLIENT")
                print(f"Time: {datetime.now().strftime('%H:%M:%S.%f')}")
                print(f"Sequence ID: {seq_id}, Length: {len(payload)}")
                
                # Try to decode
                if len(payload) > 0:
                    first_byte = payload[0]
                    
                    if first_byte == 0x00:  # OK packet or binary row
                        if in_result_set and col_count > 0:
                            print("Type: Binary Result Row")
                            col_types = [c['type_code'] for c in col_definitions]
                            values = decode_binary_row(payload, col_count, col_types)
                            if values:
                                print("Values:")
                                for col, (val_type, val) in zip(col_definitions, values):
                                    print(f"  {col['name']}: {val_type} = {val}")
                        else:
                            print("Type: OK Packet")
                    elif first_byte == 0xFF:  # Error
                        print("Type: Error Packet")
                    elif first_byte == 0xFE and len(payload) < 9:  # EOF
                        print("Type: EOF Packet")
                        if in_result_set and len(col_definitions) > 0:
                            in_result_set = False
                    else:
                        # Could be column count, column def, or text row
                        if not in_result_set:
                            # Might be column count
                            col_count, _ = decode_lenenc_int(payload, 0)
                            if col_count and col_count < 1000:
                                print(f"Type: Column Count ({col_count} columns)")
                                in_result_set = True
                                col_definitions = []
                        elif len(col_definitions) < col_count:
                            # Column definition
                            col_def = decode_column_definition(payload)
                            if col_def:
                                col_definitions.append(col_def)
                                print(f"Type: Column Definition")
                                print(f"  Name: {col_def['name']}")
                                print(f"  Type: {col_def['type']} (code={col_def['type_code']})")
                                print(f"  Charset: {col_def['charset']}")
                                print(f"  Length: {col_def['length']}")
                                print(f"  Flags: {col_def['flags']:04x} ({', '.join(col_def['flag_names'])})")
                                print(f"  Decimals: {col_def['decimals']}")
                        else:
                            # Text result row
                            print("Type: Text Result Row")
                            values = decode_text_row(payload, col_count)
                            if values:
                                print("Values:")
                                for col, (val_type, val) in zip(col_definitions, values):
                                    print(f"  {col['name']}: {val_type} = {repr(val)}")
                
                hex_dump(payload)
                
                # Forward to client
                client_sock.sendall(full_packet)

if __name__ == '__main__':
    try:
        proxy_connection()
    except KeyboardInterrupt:
        print("\n\nProxy stopped")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
