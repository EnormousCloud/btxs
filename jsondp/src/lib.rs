use anyhow::{bail, Context};
use serde_json::{Map, Number, Value};
use std::io::{BufWriter, Read, Write};

pub mod decode;
pub mod dictionary;
pub mod encode;

use decode::*;
use dictionary::*;

pub fn decode_object<R: Read, D1: DictionaryRead, D2: DictionaryRead>(
    input: &mut R,
    size: usize,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<Map<String, Value>> {
    let mut m = Map::new();
    for _ in 0..size {
        let nb = next_u8(input)?;
        let use_fd = (nb & 0xc0) > 0;

        let fprefix = nb & 0x1F;
        let field = if use_fd {
            let dict_id = if (nb & 0xc0) == 0xc0 {
                next_u32(input)?
            } else if (nb & 0xc0) == 0x80 {
                next_u16(input)? as u32
            } else {
                next_u8(input)? as u32
            };
            match fd.get(dict_id) {
                Some(found) => std::str::from_utf8(found)?.to_string(),
                None => bail!(format!("field value {} not found in dictionary", dict_id)),
            }
        } else if fprefix == 20 {
            // expect fprefix to be field name... but thus could be a number actually
            let sz = next_u8(input)? as usize;
            next_str(input, sz)?
        } else {
            bail!("only short strings are supported as column names so far");
        };
        println!("DECODED OBJECT FIELD {:?}", field);
        let value = decode(input, fd, vd)?;
        println!("DECODED OBJECT VALUE {:?}", value);
        m.insert(field, value);
    }
    println!("MAP {:?}", m);
    Ok(m)
}

/// converts encoded bytes from Buffer into JSON value,
/// using given field and value dictionaries
pub fn decode<R: Read, D1: DictionaryRead, D2: DictionaryRead>(
    input: &mut R,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<Value> {
    let nb = next_u8(input)?;
    let use_vd = (nb & 0x20) > 0;
    println!("NB={} USE VD={}", nb, use_vd);
    match nb & 0x1F {
        0 => Ok(Value::Bool(false)),
        1 => Ok(Value::Bool(true)),
        2 => Ok(Value::Number(Number::from(next_u8(input)?))),
        3 => Ok(Value::Number(Number::from(next_i8(input)?))),
        4 => Ok(Value::String(format!(
            "0x{}",
            hex::encode(&[next_u8(input)?])
        ))),
        5 => Ok(Value::Number(Number::from(next_u16(input)?))),
        6 => Ok(Value::Number(Number::from(next_i16(input)?))),
        7 => Ok(Value::Number(Number::from(next_u32(input)?))),
        8 => Ok(Value::Number(Number::from(next_i32(input)?))),
        9 => Ok(Value::Number(Number::from(next_u64(input)?))),
        10 => Ok(Value::Number(Number::from(next_i64(input)?))),
        11 => Ok(Value::String(format!(
            "0x{}",
            hex::encode(next_u64(input)?.to_be_bytes()),
        ))),
        12 => Ok(Value::String(format!(
            "0x{}",
            hex::encode(next_u16(input)?.to_le_bytes()),
        ))),
        13 => Ok(Value::String(format!(
            "0x{}",
            hex::encode(next_u32(input)?.to_be_bytes()),
        ))),
        14 => Ok(Value::String(format!(
            "0x{}",
            hex::encode(next_u128(input)?.to_be_bytes()),
        ))),
        15 => {
            let lo = next_u32(input)?;
            let hi = next_u128(input)?;
            Ok(Value::String(format!(
                "0x{}{}",
                hex::encode(hi.to_be_bytes()),
                hex::encode(lo.to_be_bytes()),
            )))
        }
        16 => {
            let lo = next_u128(input)?;
            let hi = next_u128(input)?;
            Ok(Value::String(format!(
                "0x{}{}",
                hex::encode(hi.to_be_bytes()),
                hex::encode(lo.to_be_bytes()),
            )))
        }
        17 => {
            let n = Number::from_f64(next_f64(input)?).context("no item")?;
            Ok(Value::Number(n))
        }
        18 => Ok(Value::Number(Number::from(0))),
        19 => {
            let size = next_u8(input)? as usize;
            let mut buf = BufWriter::new(Vec::new());
            next(input, size, &mut buf)?;
            let b = buf.into_inner()?;
            Ok(Value::String(format!("0x{}", hex::encode(&b))))
        }
        20 => {
            if use_vd {
                let dict_id = next_u32(input)?;
                if let Some(buf) = vd.get(dict_id) {
                    let s = std::str::from_utf8(buf)?.to_string();
                    return Ok(Value::String(s));
                }
            }
            let size = next_u8(input)? as usize;
            let mut buf = BufWriter::new(Vec::new());
            next(input, size, &mut buf)?;
            let s = String::from_utf8(buf.into_inner()?)?;
            Ok(Value::String(s))
        }
        23 => {
            let size = next_u16(input)? as usize;
            let mut buf = BufWriter::new(Vec::new());
            next(input, size, &mut buf)?;
            let b = buf.into_inner().unwrap();
            Ok(Value::String(format!("0x{}", hex::encode(&b))))
        }
        24 => {
            let size = next_u16(input)? as usize;
            let mut buf = BufWriter::new(Vec::new());
            next(input, size, &mut buf)?;
            let s = String::from_utf8(buf.into_inner().unwrap()).unwrap();
            Ok(Value::String(s))
        }
        21 => {
            let size = next_u8(input)? as usize;
            let mut vals = Vec::new();
            for _ in 0..size {
                vals.push(decode(input, fd, vd)?);
            }
            Ok(Value::Array(vals))
        }
        25 => {
            let size = next_u16(input)? as usize;
            let mut vals = Vec::new();
            for _ in 0..size {
                vals.push(decode(input, fd, vd)?);
            }
            Ok(Value::Array(vals))
        }
        22 => {
            let size = next_u8(input)? as usize;
            println!("DECODING OBJECT OF SIZE {}", size);
            Ok(Value::Object(decode_object(input, size, fd, vd)?))
        }
        26 => {
            let size = next_u16(input)? as usize;
            Ok(Value::Object(decode_object(input, size, fd, vd)?))
        }
        31 => Ok(Value::Null),
        _ => bail!("invalid field type"),
    }
}

/// converts JSON value into encoded bytes using given writer,
/// field and value dictionaries
pub fn encode<W: Write, D1: DictionaryRead, D2: DictionaryRead>(
    input: &Value,
    w: &mut W,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<()> {
    encode::encode_value(input, w, fd, vd)?;
    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::io::{BufReader, BufWriter};
    use std::str::FromStr;

    // dictionary for testing
    const D: &'static [&'static str] = &[
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "0x95087266018b9637aff3d76d4e0cad7e52c19636",
    ];

    // encode without dictionary
    fn enc(input: &Value) -> anyhow::Result<Vec<u8>> {
        let nod = NoDictionary {};
        let mut buf = BufWriter::new(Vec::new());
        encode::encode_value(input, &mut buf, &nod, &nod).unwrap();
        let v = buf.into_inner().unwrap();
        Ok(v)
    }

    // encode with sample dictionary
    fn enc_d(input: &Value) -> anyhow::Result<Vec<u8>> {
        let d = MapDictionary::from_static(D);
        let mut buf = BufWriter::new(Vec::new());
        encode::encode_value(input, &mut buf, &d, &d).unwrap();
        let v = buf.into_inner().unwrap();
        Ok(v)
    }

    // decode without dictionary
    fn dec(input: &[u8]) -> anyhow::Result<Value> {
        let nod = NoDictionary {};
        let mut buf = BufReader::new(input);
        decode(&mut buf, &nod, &nod)
    }

    // decode with sample dictionary
    fn dec_d(input: &[u8]) -> anyhow::Result<Value> {
        let d = MapDictionary::from_static(D);
        let mut buf = BufReader::new(input);
        decode(&mut buf, &d, &d)
    }

    #[test]
    fn it_encodes_decodes_bool_and_null() {
        let t = enc(&json!(true)).unwrap();
        assert_eq!(dec(&t).unwrap().as_bool().unwrap(), true);
        let f = enc(&json!(false)).unwrap();
        assert_eq!(dec(&f).unwrap().as_bool().unwrap(), false);
        let n = enc(&json!(null)).unwrap();
        assert!(dec(&n).unwrap().is_null());
    }

    #[test]
    fn it_encodes_decodes_numbers() {
        let z = enc(&json!(0)).unwrap();
        assert_eq!(dec(&z).unwrap().as_u64().unwrap(), 0);
        let o = enc(&json!(1)).unwrap();
        assert_eq!(dec(&o).unwrap().as_u64().unwrap(), 1);
        let mo = enc(&json!(-1)).unwrap();
        assert_eq!(dec(&mo).unwrap().as_i64().unwrap(), -1);
        let x1 = enc(&json!("0x01")).unwrap();
        assert_eq!(dec(&x1).unwrap().as_str().unwrap(), "0x01");
        let m3 = enc(&json!(-300)).unwrap();
        assert_eq!(dec(&m3).unwrap().as_i64().unwrap(), -300);
        let p3 = enc(&json!(300)).unwrap();
        assert_eq!(dec(&p3).unwrap().as_i64().unwrap(), 300);
        let x10 = enc(&json!("0x100")).unwrap();
        assert_eq!(dec(&x10).unwrap().as_str().unwrap(), "0x0100");
        let m5 = enc(&json!(-50000)).unwrap();
        assert_eq!(dec(&m5).unwrap().as_i64().unwrap(), -50000);
        let p5 = enc(&json!(50000)).unwrap();
        assert_eq!(dec(&p5).unwrap().as_i64().unwrap(), 50000);
        let x100 = enc(&json!("0x1a2b3c")).unwrap();
        assert_eq!(dec(&x100).unwrap().as_str().unwrap(), "0x001a2b3c");
        let xl0 = enc(&json!("0x1a0000ffeeddff00")).unwrap();
        assert_eq!(dec(&xl0).unwrap().as_str().unwrap(), "0x1a0000ffeeddff00");
        let hp = enc(&json!(50u64 + u32::MAX as u64)).unwrap();
        assert_eq!(dec(&hp).unwrap().as_i64().unwrap(), 4294967345);
        let hm = enc(&json!(-50i64 + i32::MIN as i64)).unwrap();
        assert_eq!(dec(&hm).unwrap().as_i64().unwrap(), -2147483698);
    }

    #[test]
    fn it_encodes_decodes_strings() {
        let alpha = enc_d(&json!("alpha")).unwrap();
        assert_eq!(dec_d(&alpha).unwrap().as_str().unwrap(), "alpha");

        let hello = enc(&json!("Hello")).unwrap();
        assert_eq!(dec(&hello).unwrap().as_str().unwrap(), "Hello");

        let lipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent nec magna fermentum, tincidunt orci quis, mollis lacus. Phasellus vitae vestibulum purus. Suspendisse sit amet lacus a nisl condimentum eleifend. Sed magna lectus, placerat ac sapien ac, interdum pellentesque sapien. Praesent eleifend, odio sit amet dignissim imperdiet, nunc risus laoreet urna, nec egestas quam nibh sit amet massa. Morbi lacinia molestie elit, nec sollicitudin erat. Vestibulum accumsan neque et ornare turpis duis.";
        let lorem = enc(&json!(lipsum)).unwrap();
        assert_eq!(dec(&lorem).unwrap().as_str().unwrap(), lipsum);
    }

    #[test]
    fn it_encodes_decodes_hex() {
        let longhex = "0x95087266018b9637aff3d76d4e0cad7eff52c10963600a895087266018b9637aff3d76d4e0cad7eff52c10963600a8";
        let lh = enc(&json!(longhex)).unwrap();
        assert_eq!(dec(&lh).unwrap().as_str().unwrap(), longhex);

        let addr = "0x95087266018b9637aff3d76d4e0cad7e52c19636";
        let just_addr = enc(&json!(addr)).unwrap();
        assert_eq!(dec(&just_addr).unwrap().as_str().unwrap(), addr);

        let dict_addr = enc_d(&json!(addr)).unwrap();
        assert_eq!(dec_d(&dict_addr).unwrap().as_str().unwrap(), addr);
    }

    #[test]
    fn it_encodes_decodes_floats() {
        let z = enc(&json!(0.0)).unwrap();
        assert_eq!(dec(&z).unwrap().as_f64().unwrap(), 0.0);
        let o = enc(&json!(-1.0)).unwrap();
        assert_eq!(dec(&o).unwrap().as_f64().unwrap(), -1.0);
        let e = enc(&json!(1.5e10)).unwrap();
        assert_eq!(dec(&e).unwrap().as_f64().unwrap(), 1.5e10);
    }

    #[test]
    fn it_encodes_decodes_array() {
        let z = enc(&json!([0, 1, 2, 3])).unwrap();
        let out_z = dec(&z).unwrap().to_string();
        assert_eq!(out_z, "[0,1,2,3]");

        let wd = enc_d(&json!(["alpha", 1, "omega"])).unwrap();
        println!("{:?}", wd);
        assert_eq!(dec_d(&wd).unwrap().to_string(), "[\"alpha\",1,\"omega\"]");
    }

    #[test]
    fn it_encodes_decodes_object() {
        let i1 = "{\"one\":\"example\",\"two\":1}";
        let v = Value::from_str(i1).unwrap();
        let encoded = enc(&v).unwrap();
        assert_eq!(dec(&encoded).unwrap().to_string(), i1);

        let s2 =
            "{\"alpha\":\"test\",\"beta\":1,\"epsilon\":{\"gamma\":\"hello\"},\"no\":\"0x01ff\"}";
        let v2 = Value::from_str(s2).unwrap();
        let d2 = enc_d(&v2).unwrap();
        println!("{}", hex::encode(&d2));
        assert_eq!(dec_d(&d2).unwrap().to_string(), s2);
    }

    #[test]
    fn it_decodes_bignumber() {
        // BN: it parsed into object
        let v = Value::from_str("{\"type\": \"BigNumber\", \"hex\": \"0x1ff\"}").unwrap();
        let bn = enc(&v).unwrap();
        assert_eq!(dec(&bn).unwrap().as_str().unwrap(), "0x01ff");

        let v2 = Value::from_str("{\"type\": \"BigNumber\", \"hex\": \"0xeeddcc1ff\"}").unwrap();
        let bn2 = enc(&v2).unwrap();
        assert_eq!(dec(&bn2).unwrap().as_str().unwrap(), "0x0eeddcc1ff");
    }
}
