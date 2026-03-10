use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::info;
use crate::sql::executor::{Executor, QueryResult};
use crate::storage::Value;
use super::packet::*;

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    // 1. Send Initial Handshake Packet
    send_handshake(&mut socket).await?;

    // 2. Receive Handshake Response
    let (_seq, _payload) = read_packet(&mut socket).await?;
    // For now, we accept any credentials (no auth)
    send_ok(&mut socket, 0).await?;

    // 3. Command Loop
    loop {
        let (seq, payload) = match read_packet(&mut socket).await {
            Ok(p) => p,
            Err(_) => break, // Connection closed
        };

        if payload.is_empty() {
            break;
        }

        let command = payload[0];
        let data = &payload[1..];

        match command {
            0x01 => break, // COM_QUIT
            0x03 => { // COM_QUERY
                let query = match std::str::from_utf8(data) {
                    Ok(q) => q,
                    Err(_) => {
                        send_error(&mut socket, seq + 1, 1105, "HY000", "Invalid UTF-8 query").await?;
                        continue;
                    }
                };
                match executor.execute(query, vec![], None).await {
                    Ok(result) => {
                        if result.rows.is_empty() {
                            send_ok(&mut socket, seq + 1).await?;
                        } else {
                            send_result_set(&mut socket, seq + 1, result).await?;
                        }
                    }
                    Err(e) => {
                        send_error(&mut socket, seq + 1, 1105, "HY000", &e.to_string()).await?;
                    }
                }
            }
            0x0E => { // COM_PING
                send_ok(&mut socket, seq + 1).await?;
            }
            _ => {
                info!("Unsupported MySQL command: 0x{:02X}", command);
                send_error(&mut socket, seq + 1, 1047, "08S01", "Unknown command").await?;
            }
        }
    }

    Ok(())
}

async fn send_handshake(socket: &mut TcpStream) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(10); // Protocol version
    payload.extend_from_slice(b"thy-squeal-0.4.0\0");
    payload.extend_from_slice(&[0u8; 4]); // Connection ID (dummy)
    payload.extend_from_slice(b"authplug\0"); // Auth plugin data part 1
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0xF7FF)?; // Capability flags (lower)
    payload.push(33); // Character set (utf8_general_ci)
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x8000)?; // Capability flags (upper)
    payload.push(0); // Auth plugin data length
    payload.extend_from_slice(&[0u8; 10]); // Reserved
    payload.extend_from_slice(b"authplug\0"); // Auth plugin data part 2
    payload.extend_from_slice(b"mysql_native_password\0");

    send_packet(socket, 0, &payload).await
}

async fn send_ok(socket: &mut TcpStream, seq: u8) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0x00); // OK header
    payload.push(0); // Affected rows
    payload.push(0); // Last insert ID
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0000)?; // Warnings
    
    send_packet(socket, seq, &payload).await
}

async fn send_error(socket: &mut TcpStream, seq: u8, code: u16, state: &str, msg: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0xFF); // Error header
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, code)?;
    payload.push(b'#'); // SQL State marker
    payload.extend_from_slice(state.as_bytes());
    payload.extend_from_slice(msg.as_bytes());
    
    send_packet(socket, seq, &payload).await
}

async fn send_result_set(socket: &mut TcpStream, mut seq: u8, result: QueryResult) -> Result<()> {
    // 1. Column Count
    let mut payload = Vec::new();
    write_len_enc_int(&mut payload, result.columns.len() as u64);
    send_packet(socket, seq, &payload).await?;
    seq += 1;

    // 2. Column Definitions
    for col_name in &result.columns {
        let mut payload = Vec::new();
        write_len_enc_str(&mut payload, "def"); // Catalog
        write_len_enc_str(&mut payload, "");    // Schema
        write_len_enc_str(&mut payload, "");    // Table
        write_len_enc_str(&mut payload, "");    // Org Table
        write_len_enc_str(&mut payload, col_name); // Name
        write_len_enc_str(&mut payload, col_name); // Org Name
        payload.push(0x0C); // Length of fixed-length fields
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 33)?; // Character set
        WriteBytesExt::write_u32::<LittleEndian>(&mut payload, 255)?; // Column length
        payload.push(0xFD); // Type (VAR_STRING)
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Flags
        payload.push(0x00); // Decimals
        WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Filter
        
        send_packet(socket, seq, &payload).await?;
        seq += 1;
    }

    // 3. EOF Packet
    send_eof(socket, seq).await?;
    seq += 1;

    // 4. Row Data
    for row in result.rows {
        let mut payload = Vec::new();
        for val in row {
            match val {
                Value::Null => payload.push(0xFB),
                _ => write_len_enc_str(&mut payload, &val.to_string()),
            }
        }
        send_packet(socket, seq, &payload).await?;
        seq += 1;
    }

    // 5. Final EOF Packet
    send_eof(socket, seq).await?;

    Ok(())
}

async fn send_eof(socket: &mut TcpStream, seq: u8) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0xFE); // EOF header
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0)?; // Warnings
    WriteBytesExt::write_u16::<LittleEndian>(&mut payload, 0x0002)?; // Status flags
    
    send_packet(socket, seq, &payload).await
}
