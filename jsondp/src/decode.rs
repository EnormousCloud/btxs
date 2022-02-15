use std::io::{BufWriter, Read, Write};

pub(crate) fn next_i8<R: Read>(input: &mut R) -> anyhow::Result<i8> {
    Ok(next_u8(input)? as i8)
}

pub(crate) fn next_u8<R: Read>(input: &mut R) -> anyhow::Result<u8> {
    let mut first = [0; 1];
    input.read_exact(&mut first)?; // read exactly 1 byte
    Ok(first[0])
}

pub(crate) fn next_i16<R: Read>(input: &mut R) -> anyhow::Result<i16> {
    Ok(next_u16(input)? as i16)
}

pub(crate) fn next_u16<R: Read>(input: &mut R) -> anyhow::Result<u16> {
    let mut first = [0; 2];
    input.read_exact(&mut first)?; // read exactly 2 bytes
    let out: u16 = (first[1] as u16) << 8 | (first[0] as u16);
    Ok(out)
}

pub(crate) fn next_i32<R: Read>(input: &mut R) -> anyhow::Result<i32> {
    Ok(next_u32(input)? as i32)
}

pub(crate) fn next_u32<R: Read>(input: &mut R) -> anyhow::Result<u32> {
    let mut first = [0; 4];
    input.read_exact(&mut first)?; // read exactly 2 bytes
    let out: u32 = (first[3] as u32) << 24
        | (first[2] as u32) << 16
        | (first[1] as u32) << 8
        | (first[0] as u32);
    Ok(out)
}

pub(crate) fn next_f64<R: Read>(input: &mut R) -> anyhow::Result<f64> {
    Ok(f64::from_bits(next_u64(input)?))
}

pub(crate) fn next_i64<R: Read>(input: &mut R) -> anyhow::Result<i64> {
    Ok(next_u64(input)? as i64)
}

pub(crate) fn next_u64<R: Read>(input: &mut R) -> anyhow::Result<u64> {
    let mut first = [0; 8];
    input.read_exact(&mut first)?; // read exactly 2 bytes
    let out: u64 = (first[7] as u64) << 56
        | (first[6] as u64) << 48
        | (first[5] as u64) << 40
        | (first[4] as u64) << 32
        | (first[3] as u64) << 24
        | (first[2] as u64) << 16
        | (first[1] as u64) << 8
        | (first[0] as u64);
    Ok(out)
}

pub(crate) fn next_u128<R: Read>(input: &mut R) -> anyhow::Result<u128> {
    let mut first = [0; 16];
    input.read_exact(&mut first)?; // read exactly 2 bytes
    let out: u128 = (first[15] as u128) << 120
        | (first[14] as u128) << 112
        | (first[13] as u128) << 104
        | (first[12] as u128) << 96
        | (first[11] as u128) << 88
        | (first[10] as u128) << 80
        | (first[9] as u128) << 72
        | (first[8] as u128) << 64
        | (first[7] as u128) << 56
        | (first[6] as u128) << 48
        | (first[5] as u128) << 40
        | (first[4] as u128) << 32
        | (first[3] as u128) << 24
        | (first[2] as u128) << 16
        | (first[1] as u128) << 8
        | (first[0] as u128);
    Ok(out)
}

pub(crate) fn next<R: Read, W: Write>(
    input: &mut R,
    bytes_to_read: usize,
    w: &mut W,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; bytes_to_read];
    input.read_exact(&mut buf)?; // read exactly N bytes
    w.write(&buf)?;
    Ok(())
}

pub(crate) fn next_str<R: Read>(input: &mut R, bytes_to_read: usize) -> anyhow::Result<String> {
    let mut buf = BufWriter::new(vec![]);
    next(input, bytes_to_read, &mut buf)?;
    let b = buf.into_inner()?;
    Ok(String::from_utf8(b)?)
}
