pub const WAL_HEADER_SIZE: usize = 4 + 4 + 4 + 1;
pub const OP_PUT: u8 = 0;
pub const OP_DELETE: u8 = 1;

pub fn encode_u32(val: u32)->[u8;4]{
    val.to_le_bytes()
}
pub fn decode_u32(bytes: &[u8]) -> u32{
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}
pub fn encode_u64(val:u64)->[u8,8]{
    val.to_le_bytes()
}
pub fn decode_u64(bytes: &[u8]-> u64){
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



