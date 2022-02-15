use crate::dictionary::*;
use anyhow::Context;
use num::ToPrimitive;
use serde_json::{Map, Number, Value};
use std::io::Write;

fn encode_string<W: Write, D: DictionaryRead>(
    value: &str,
    w: &mut W,
    vd: &D,
) -> anyhow::Result<()> {
    // try to read "0x" as hex bytes
    if with_0x(value.as_bytes()) {
        let mut remainer: Vec<u8> = value.bytes().skip(2).collect();
        let mut hexchars = if remainer.len() % 2 == 0 {
            vec![]
        } else {
            vec![48] // '0'
        };
        hexchars.append(&mut remainer);
        let out: Vec<u8> = hex::decode(&hexchars).context("hex decoding")?;
        if out.len() > 256 {
            let size: u16 = value.len() as u16;
            let ch: u8 = byte_prefix(FieldType::DWB { size });
            let lo: u8 = (size & 0xFF) as u8;
            let hi: u8 = (size >> 8) as u8;
            w.write(&[ch]).context("write dwb prefix")?;
            w.write(&[lo, hi]).context("write dwb len")?;
            w.write(&out).context("write dwb value")?;
        } else {
            let bytes_num: u8 = out.len() as u8;
            if bytes_num == 1 {
                let ch: u8 = byte_prefix(FieldType::B8);
                w.write(&[ch]).context("write db prefix")?;
                w.write(&out).context("write db value")?;
            } else if bytes_num == 2 {
                let ch: u8 = byte_prefix(FieldType::B16);
                let b16 = out[0] as u16 | ((out[1] as u16) << 8);
                w.write(&[ch]).context("write db prefix")?;
                w.write(&b16.to_le_bytes()).context("write db value")?;
            } else if bytes_num <= 4 {
                let ch: u8 = byte_prefix(FieldType::B32);
                let rev: Vec<u8> = out.iter().copied().rev().collect();
                w.write(&[ch]).context("write db prefix")?;
                w.write(&rev).context("write db value")?; // WRONG
                for _ in 0..(4 - rev.len()) {
                    w.write(&[0]).context("write db4 alignment")?;
                }
            } else if bytes_num <= 8 {
                let ch: u8 = byte_prefix(FieldType::B64);
                let rev: Vec<u8> = out.iter().copied().rev().collect();
                w.write(&[ch]).context("write db prefix")?;
                w.write(&rev).context("write db value")?;
                for _ in 0..(8 - rev.len()) {
                    w.write(&[0]).context("write db8 alignment")?;
                }
            } else if bytes_num <= 16 {
                let ch: u8 = byte_prefix(FieldType::B128);
                let rev: Vec<u8> = out.iter().copied().rev().collect();
                w.write(&[ch]).context("write db prefix")?;
                w.write(&rev).context("write db value")?;
                for _ in 0..(16 - rev.len()) {
                    w.write(&[0]).context("write db16 alignment")?;
                }
            } else if bytes_num <= 20 {
                let ch: u8 = byte_prefix(FieldType::B160);
                let rev: Vec<u8> = out.iter().copied().rev().collect();
                w.write(&[ch]).context("write db prefix")?;
                w.write(&rev).context("write db value")?;
                for _ in 0..(20 - rev.len()) {
                    w.write(&[0]).context("write db20 alignment")?;
                }
            } else if bytes_num <= 32 {
                let ch: u8 = byte_prefix(FieldType::B256);
                let rev: Vec<u8> = out.iter().copied().rev().collect();
                w.write(&[ch]).context("write db prefix")?;
                w.write(&rev).context("write db value")?;
                for _ in 0..(32 - rev.len()) {
                    w.write(&[0]).context("write db32 alignment")?;
                }
            } else {
                // in this case we are preserving the order
                let ch: u8 = byte_prefix(FieldType::DB { size: bytes_num });
                w.write(&[ch]).context("write db prefix")?;
                w.write(&[bytes_num]).context("write db len")?;
                w.write(&out).context("write db value")?;
            }
        }
        return Ok(());
    }

    if value.len() > 256 {
        let size: u16 = value.len() as u16;
        let ch: u8 = byte_prefix(FieldType::DWS { size });
        let lo: u8 = (size & 0xFF) as u8;
        let hi: u8 = (size >> 8) as u8;
        w.write(&[ch]).context("write ds prefix")?;
        w.write(&[lo, hi]).context("write ds len")?;
        w.write(value.as_bytes()).context("write ds value")?;
        return Ok(());
    }
    let size: u8 = value.len() as u8;
    let ch = byte_prefix(FieldType::DS { size });
    match vd.find_str(value) {
        Some(dict_id) => {
            // we are lucky to have that value in a dictionary, dictionary is always u32?
            w.write(&[ch | 0x20]).context("write str dict prefix")?;
            w.write(&dict_id.to_le_bytes()).context("write str dict")?; // might not work
        }
        None => {
            // we didn't manage to find that value in the dictionary
            w.write(&[ch, value.len() as u8])
                .context("write str prefix")?;
            w.write(value.as_bytes()).context("write str")?;
        }
    }
    Ok(())
}

fn encode_array<W: Write, D1: DictionaryRead, D2: DictionaryRead>(
    value: &Vec<Value>,
    w: &mut W,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<()> {
    if value.len() > 256 {
        let size: u16 = value.len() as u16;
        let ch: u8 = byte_prefix(FieldType::DWA { size });
        w.write(&[ch]).context("write dwa prefix")?;
        w.write(&size.to_le_bytes()).context("write dwa len")?;
    } else {
        let size: u8 = value.len() as u8;
        let ch = byte_prefix(FieldType::DA { size });
        w.write(&[ch, size]).context("write da")?;
    }
    for item in value {
        encode_value(item, w, fd, vd)?;
    }
    Ok(())
}

fn with_0x(input: &[u8]) -> bool {
    input.len() > 2 && input[0] == ('0' as u8) && input[1] == ('x' as u8)
}

fn big_number(value: &Map<String, Value>) -> anyhow::Result<Option<Vec<u8>>> {
    if value.len() == 2 {
        if let Some(t) = value.get("type") {
            if let Value::String(s) = t {
                if s.as_str() != "BigNumber" {
                    return Ok(None);
                }
            }
        }
        if let Some(v) = value.get("hex") {
            if let Value::String(s) = v {
                let hexstr = s.as_str().as_bytes();
                if !with_0x(hexstr) {
                    return Ok(None);
                }
                let mut remained: Vec<u8> = s.as_str().bytes().skip(2).collect();
                let mut hexchars = if remained.len() % 2 == 0 {
                    vec![]
                } else {
                    vec![48] // '0'
                };
                hexchars.append(&mut remained);
                let out: Vec<u8> = hex::decode(&hexchars)?;
                return Ok(Some(out));
            }
        }
    }
    Ok(None)
}

fn encode_object<W: Write, D1: DictionaryRead, D2: DictionaryRead>(
    value: &Map<String, Value>,
    w: &mut W,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<()> {
    if let Ok(Some(out)) = big_number(value) {
        // treat known objects, like BigNumber specially; should be just bytes
        let size: u8 = out.len() as u8;
        let ch: u8 = byte_prefix(FieldType::DB { size });
        w.write(&[ch]).context("write bn db prefix")?;
        w.write(&[size]).context("write bn db len")?;
        w.write(&out).context("write bn db value")?;
        return Ok(());
    }

    let size: u8 = value.len() as u8;
    let ch = byte_prefix(FieldType::DO { size });
    w.write(&[ch, size]).context("write do")?;
    for (k, v) in value {
        match fd.find_str(&k.as_str()) {
            Some(dict_id) => {
                if dict_id > std::u16::MAX as u32 {
                    w.write(&[0xc0 | byte_prefix(FieldType::U32)])
                        .context("do u32 prefix")?;
                    w.write(&dict_id.to_le_bytes()).context("do u32")?;
                } else if dict_id > std::u8::MAX as u32 {
                    w.write(&[0x80 | byte_prefix(FieldType::U16)])
                        .context("do u16 prefix")?;
                    w.write(&(dict_id as u16).to_le_bytes()).context("do u16")?;
                } else {
                    w.write(&[0x40 | byte_prefix(FieldType::U8)])
                        .context("do u8 prefix")?;
                    w.write(&[dict_id as u8]).context("do u8")?;
                };
            }
            None => {
                encode_string(k.as_str(), w, vd)?;
            }
        };
        encode_value(&v, w, fd, vd)?;
    }
    Ok(())
}

fn encode_number<W: Write>(value: &Number, w: &mut W) -> anyhow::Result<()> {
    if value.is_i64() {
        let v: i64 = value.as_i64().context("bad i64")?;
        if v == 0i64 {
            let ch = byte_prefix(FieldType::ZERO);
            w.write(&[ch]).context("write 0i64")?;
        } else if let Some(v8) = v.to_i8() {
            let ch = byte_prefix(FieldType::I8);
            w.write(&[ch, v8 as u8]).context("write i8")?;
        } else if let Some(v16) = v.to_i16() {
            let ch = byte_prefix(FieldType::I16);
            let lo: u8 = (v16 & 0xFF) as u8;
            let hi: u8 = (v16 >> 8) as u8;
            w.write(&[ch, lo, hi]).context("write i16")?;
        } else if let Some(v32) = v.to_i32() {
            let ch = byte_prefix(FieldType::I32);
            w.write(&[ch]).context("write i32 prefix")?;
            w.write(&v32.to_le_bytes()).context("write i32")?;
        } else {
            let ch = byte_prefix(FieldType::I64);
            w.write(&[ch]).context("write i64 prefix")?;
            w.write(&v.to_le_bytes()).context("write i64")?;
        }
    } else if value.is_u64() {
        let v: u64 = value.as_u64().context("bad u64")?;
        if v == 0u64 {
            let ch = byte_prefix(FieldType::ZERO);
            w.write(&[ch]).context("write 0u64")?;
        } else if let Some(v8) = v.to_u8() {
            let ch = byte_prefix(FieldType::U8);
            w.write(&[ch, v8]).context("write u8")?;
        } else if let Some(v16) = v.to_u16() {
            let ch = byte_prefix(FieldType::U16);
            let lo: u8 = (v16 & 0xFF) as u8;
            let hi: u8 = (v16 >> 8) as u8;
            w.write(&[ch, lo, hi]).context("write u16")?;
        } else if let Some(v32) = v.to_u32() {
            let ch = byte_prefix(FieldType::U32);
            w.write(&[ch]).context("write u32 prefix")?;
            w.write(&v32.to_le_bytes()).context("write u32")?;
        } else {
            let ch = byte_prefix(FieldType::U64);
            w.write(&[ch]).context("write u64 prefix")?;
            w.write(&v.to_le_bytes()).context("write u64")?;
        }
    } else if value.is_f64() {
        let ch = byte_prefix(FieldType::F64);
        let b = value.as_f64().context("f64")?.to_le_bytes();
        w.write(&[ch]).context("write f64 prefix")?;
        w.write(&b).context("write f64")?;
    } else {
        return Err(anyhow::Error::msg("number parsing failure"));
    };

    Ok(())
}

pub(crate) fn encode_value<W: Write, D1: DictionaryRead, D2: DictionaryRead>(
    input: &Value,
    w: &mut W,
    fd: &D1,
    vd: &D2,
) -> anyhow::Result<()> {
    match input {
        Value::Null => {
            let ch: u8 = byte_prefix(FieldType::NULL);
            w.write(&[ch]).context("write null")?;
        }
        Value::Bool(value) => {
            let ch: u8 = byte_prefix(if *value {
                FieldType::TRUE
            } else {
                FieldType::FALSE
            });
            w.write(&[ch]).context("write bool")?;
        }
        Value::Number(value) => {
            encode_number(value, w)?;
        }
        Value::String(value) => {
            encode_string(&value.as_str(), w, vd)?;
        }
        Value::Array(value) => {
            encode_array(value, w, fd, vd)?;
        }
        Value::Object(value) => {
            encode_object(value, w, fd, vd)?;
        }
    };
    Ok(())
}

#[derive(Debug, Clone)]
pub enum FieldType {
    FALSE,
    TRUE,
    U8,
    I8,
    B8,
    U16,
    I16,
    B16,
    U32,
    I32,
    B32,
    U64,
    I64,
    B64,
    B128,
    B160,
    B256,
    F64,
    ZERO,
    DB { size: u8 },
    DS { size: u8 },
    DA { size: u8 },
    DO { size: u8 },
    DWB { size: u16 },
    DWS { size: u16 },
    DWA { size: u16 },
    DWO { size: u16 },
    NULL,
}

fn byte_prefix(input: FieldType) -> u8 {
    match input {
        FieldType::FALSE => 0,
        FieldType::TRUE => 1,
        FieldType::U8 => 2,
        FieldType::I8 => 3,
        FieldType::B8 => 4,
        FieldType::U16 => 5,
        FieldType::I16 => 6,
        FieldType::B16 => 12,
        FieldType::U32 => 7,
        FieldType::I32 => 8,
        FieldType::B32 => 13,
        FieldType::U64 => 9,
        FieldType::I64 => 10,
        FieldType::B64 => 11,
        FieldType::B128 => 14,
        FieldType::B160 => 15,
        FieldType::B256 => 16,
        FieldType::F64 => 17,
        FieldType::ZERO => 18,
        FieldType::DS { size: _ } => 20,
        FieldType::DB { size: _ } => 19,
        FieldType::DA { size: _ } => 21,
        FieldType::DO { size: _ } => 22,
        FieldType::DWS { size: _ } => 24,
        FieldType::DWB { size: _ } => 23,
        FieldType::DWA { size: _ } => 25,
        FieldType::DWO { size: _ } => 26,
        FieldType::NULL => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::BufWriter;
    use std::str::FromStr;

    // encode without dictionary
    fn enc(input: &Value) -> anyhow::Result<Vec<u8>> {
        let nod = NoDictionary {};
        let mut buf = BufWriter::new(Vec::new());
        encode_value(input, &mut buf, &nod, &nod).unwrap();
        let v = buf.into_inner().unwrap();
        Ok(v)
    }

    // encode with sample dictionary
    fn enc_d(input: &Value) -> anyhow::Result<Vec<u8>> {
        let d = MapDictionary::from_strings(vec!["alpha", "beta", "gamma", "delta", "epsilon"]);
        let mut buf = BufWriter::new(Vec::new());
        encode_value(input, &mut buf, &d, &d).unwrap();
        let v = buf.into_inner().unwrap();
        Ok(v)
    }

    #[test]
    fn it_encodes_boolean_and_null() {
        let t = enc(&json!(true)).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0], 1u8);
        let f = enc(&json!(false)).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0], 0u8);
        let n = enc(&json!(null)).unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0], 31u8);
    }

    #[test]
    fn it_encodes_numbers() {
        let z = enc(&json!(0)).unwrap();
        assert_eq!(z.len(), 1);
        assert_eq!(z[0], 18);

        // ---
        let mone = enc(&json!(-1)).unwrap();
        assert_eq!(mone.len(), 2);
        assert_eq!(mone[1], 255u8);
        let one = enc(&json!(1)).unwrap();
        assert_eq!(one.len(), 2);
        assert_eq!(one[1], 1u8);
        let x = enc(&json!("0x01")).unwrap();
        assert_eq!(x.len(), 2);
        assert_eq!(x[0], byte_prefix(FieldType::B8));
        assert_eq!(x[1], 1u8);
        // ---
        let mshort = enc(&json!(-300)).unwrap();
        assert_eq!(mshort.len(), 3);
        let short = enc(&json!(300)).unwrap();
        assert_eq!(short.len(), 3);
        let xshort = enc(&json!("0x100")).unwrap();
        assert_eq!(xshort.len(), 3);
        // ---

        let ml = enc(&json!(-50000)).unwrap();
        assert_eq!(ml.len(), 5);
        let l = enc(&json!(50000)).unwrap();
        assert_eq!(l.len(), 5);
        let xl = enc(&json!("0x1a0000")).unwrap();
        assert_eq!(xl.len(), 5);
        // ---
        let ml = enc(&json!(50u64 + u32::MAX as u64)).unwrap();
        assert_eq!(ml.len(), 9);
        let mxl = enc(&json!("0x1a0000ffeeddff00")).unwrap();
        assert_eq!(mxl.len(), 9);
        let mx = enc(&json!(-50i64 + i32::MIN as i64)).unwrap();
        assert_eq!(mx.len(), 9);
    }

    #[test]
    fn it_encodes_floats() {
        let z = enc(&json!(0.0)).unwrap();
        assert_eq!(z.len(), 9);
        let one = enc(&json!(-1.0)).unwrap();
        assert_eq!(one.len(), 9);
        let exp = enc(&json!(1.5e10)).unwrap();
        assert_eq!(exp.len(), 9);
    }

    #[test]
    fn it_encodes_big_numbers() {
        let txs = "0xd8052f44b36869fa1f193ec2c97e6a36892840635dc347554efb8778a7a3935a";
        let tx = enc(&json!(txs)).unwrap();
        assert_eq!(tx.len(), 33);
        let addr = enc(&json!("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")).unwrap();
        assert_eq!(addr.len(), 21);
        let short = enc(&json!("0x9eeeffffeeaad80510")).unwrap();
        assert_eq!(short.len(), 17);
        let l = "0x00000000d8052f44b36869fa1f193ec2c97e6a36892840635dc347554efb8778a7a3935a";
        let buf = enc(&json!(l)).unwrap();
        assert_eq!(buf.len(), 38);
        // special object
        let v = Value::from_str("{\"type\": \"BigNumber\", \"hex\": \"0x1ff\"}").unwrap();
        println!("v={:?}", v);
        let bn = enc(&v).unwrap();
        assert_eq!(bn.len(), 1 + 1 + 2);
    }

    #[test]
    fn it_encodes_strings() {
        let longs = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla ac sapien vitae diam bibendum scelerisque ut non mi. Curabitur dolor urna, porttitor et ex in, hendrerit hendrerit magna. Cras ante urna, eleifend ac convallis eu, eleifend eget nibh. Pellentesque interdum nec ante scelerisque aliquet. Cras odio velit, luctus eu eros sit amet, suscipit maximus ipsum. Mauris elementum massa eu tortor luctus, quis cursus eros tempus. Vivamus iaculis, tortor pellentesque posuere rhoncus, lectus sapien vulputate turpis, ac aliquet lacus eros sit amet ex. Curabitur quis eleifend nisl. Mauris ligula risus, rutrum sed nibh sit amet, dignissim aliquam velit. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin nec auctor tortor. Suspendisse quis sem urna. Nunc vestibulum suscipit urna, nec vulputate lorem bibendum a. Nam nec ultricies felis. Sed a neque ultricies, pulvinar odio facilisis, commodo dolor. Morbi iaculis maximus velit id scelerisque";
        let longse = enc(&json!(longs)).unwrap();
        assert_eq!(longse.len(), 3 + longs.len());

        let s = "hello, world";
        let se = enc(&json!(s)).unwrap();
        assert_eq!(se.len(), 2 + s.len());
    }

    #[test]
    fn it_encodes_object() {
        let a = enc(&json!({ "test": 10 })).unwrap();
        assert_eq!(a.len(), 1 + 1 + (2 + 4) + 2);

        let b = enc_d(&json!({ "alpha": 10 })).unwrap();
        let b2 = b[2];
        assert_eq!(b.len(), 1 + 1 + (1 + 1) + 2);
        assert!(b2 > 0x40);

        let s2 =
            "{\"alpha\":\"test\",\"beta\":1,\"omega\":{\"gamma\":\"delta\"},\"no\":\"0x01ff\"}";
        let v2 = Value::from_str(s2).unwrap();
        let d2 = enc(&v2).unwrap();
        println!("d2.len = {}", d2.len());
    }

    #[test]
    fn it_encodes_array() {
        let a = enc(&json!([0, 1, 2])).unwrap();
        assert_eq!(a.len(), 1 + 1 + (1 + 2 + 2));

        let s = enc(&json!(["one", "two", "three"])).unwrap();
        assert_eq!(s.len(), 2 + (2 + 3) + (2 + 3) + (2 + 5));

        let b = enc_d(&json!(["alpha", "beta", "gamma"])).unwrap();
        assert_eq!(b.len(), 2 + (1 + 4) + (1 + 4) + (1 + 4));
        assert!(b[2] > 0x20);
        assert!(b[2 + 5] > 0x20);
        assert!(b[2 + 5 + 5] > 0x20);
    }
}
