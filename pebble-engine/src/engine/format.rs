pub const WAL_HEADER_SIZE: usize = 4 + 4 + 4 + 1;
pub const OP_PUT: u8 = 0;
pub const OP_DELETE: u8 = 1;

pub fn encode_u32(val: u32)->[u8;4]{
    val.to_le_bytes()
}
pub fn decode_u32(bytes: &[u8]) -> u32{
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}
pub fn encode_u64(val:u64)->[u8;8]{
    val.to_le_bytes()
}
pub fn decode_u64(bytes: &[u8])-> u64{
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}
pub fn encode_wal_record(key:&[u8], value: &[u8], op: u8)->Vec<u8>{
    let key_len = key.len() as u32;
    let val_len = value.len() as u32;

    let mut payload = Vec::with_capacity(4+4+1+key.len()+value.len());
    payload.extend_from_slice(&encode_u32(key_len));
    payload.extend_from_slice(&encode_u32(val_len));
    payload.push(op);
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);

    let crc = crc32fast::hash(&payload);

    let mut record = Vec::with_capacity(4 + payload.len());
    record.extend_from_slice(&encode_u32(crc));
    record.extend_from_slice(&payload);
    record

}
pub fn decode_wal_record(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u8, usize)>{
    if data.len() < WAL_HEADER_SIZE {
        return None;
    }

    let stored_crc = decode_u32(&data[0..4]);
    let key_len = decode_u32(&data[4..8]) as usize;
    let val_len = decode_u32(&data[8..12]) as usize;
    let op = data[12];

    let total = WAL_HEADER_SIZE + key_len + val_len;
    if data.len() < total {
        return None; // truncated record
    }

    let computed_crc = crc32fast::hash(&data[4..total]);
    if computed_crc != stored_crc {
        return None; 
    }
    let key = data[WAL_HEADER_SIZE..WAL_HEADER_SIZE + key_len].to_vec();
    let val = data[WAL_HEADER_SIZE + key_len..total].to_vec();

    Some((key, val, op, total))
}
pub fn encode_sstable_record(key: &[u8], value: &[u8], op: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 4 + 1 + key.len() + value.len());
    buf.extend_from_slice(&encode_u32(key.len() as u32));
    buf.extend_from_slice(&encode_u32(value.len() as u32));
    buf.push(op);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

/// Encode an SSTable sparse index entry
pub fn encode_index_entry(key: &[u8], offset: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len() + 8);
    buf.extend_from_slice(&encode_u32(key.len() as u32));
    buf.extend_from_slice(key);
    buf.extend_from_slice(&encode_u64(offset));
    buf
}



