use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn read_packet(socket: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await?;

    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];

    let mut payload = vec![0u8; len];
    socket.read_exact(&mut payload).await?;

    Ok((seq, payload))
}

pub async fn send_packet(socket: &mut TcpStream, seq: u8, payload: &[u8]) -> Result<()> {
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

pub fn write_len_enc_int(buf: &mut Vec<u8>, val: u64) {
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

pub fn write_len_enc_str(buf: &mut Vec<u8>, s: &str) {
    write_len_enc_int(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}
