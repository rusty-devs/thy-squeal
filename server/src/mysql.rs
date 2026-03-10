use crate::sql::executor::Executor;
use crate::storage::Value;
use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

/// MySQL Protocol Handler
pub struct MySqlProtocol {
    executor: Arc<Executor>,
}

impl MySqlProtocol {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }

    pub async fn run(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("MySQL Protocol listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let executor = self.executor.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, executor).await {
                    error!("MySQL connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
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

async fn read_packet(socket: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await?;
    
    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    
    let mut payload = vec![0u8; len];
    socket.read_exact(&mut payload).await?;
    
    Ok((seq, payload))
}

async fn send_packet(socket: &mut TcpStream, seq: u8, payload: &[u8]) -> Result<()> {
    let len = payload.len();
    let mut header = [0u8; 4];
    header[0] = (len & 0xFF) as u8;
    header[1] = ((len >> 8) & 0xFF) as u8;
    header[2] = ((len >> 16) & 0xFF) as u8;
    header[3] = seq;
    
    socket.write_all(&header).await?;
    socket.write_all(payload).await?;
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

async fn send_result_set(socket: &mut TcpStream, mut seq: u8, result: crate::sql::executor::QueryResult) -> Result<()> {
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

fn write_len_enc_int(buf: &mut Vec<u8>, val: u64) {
    if val < 251 {
        buf.push(val as u8);
    } else if val < 0x10000 {
        buf.push(0xFC);
        WriteBytesExt::write_u16::<LittleEndian>(buf, val as u16).unwrap();
    } else if val < 0x1000000 {
        buf.push(0xFD);
        let bytes = (val as u32).to_le_bytes();
        buf.extend_from_slice(&bytes[..3]);
    } else {
        buf.push(0xFE);
        WriteBytesExt::write_u64::<LittleEndian>(buf, val).unwrap();
    }
}

fn write_len_enc_str(buf: &mut Vec<u8>, s: &str) {
    write_len_enc_int(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}
