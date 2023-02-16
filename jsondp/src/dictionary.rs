use anyhow::Context;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{BufRead, Read, Write};

/// Trait to extract values from the dictionary
pub trait DictionaryRead {
    fn get(&self, item_id: u32) -> Option<&[u8]>;
    fn find_str(&self, value: &str) -> Option<u32>;
}

/// Dictionary implementation that doesn't store anything ever
pub struct NoDictionary {}

impl DictionaryRead for NoDictionary {
    fn get(&self, _: u32) -> Option<&[u8]> {
        None
    }
    fn find_str(&self, _: &str) -> Option<u32> {
        None
    }
}

/// Dictionary implementation that stores dictionary in memory
#[derive(Debug, Clone)]
pub struct MapDictionary {
    v: BTreeMap<u32, String>,
    k: BTreeMap<String, u32>,
}

fn split_at_colon<'a>(s: &'a str) -> Option<(&'a str, &'a str)> {
    let i = s.find(':');
    // i is a byte index, not a character index.
    // But we know that the '+1' will work here because the UTF-8
    // representation of ':' is a single byte.
    i.map(|i| (&s[0..i], &s[i + 1..]))
}

impl MapDictionary {
    pub fn new() -> Self {
        Self {
            k: BTreeMap::new(),
            v: BTreeMap::new(),
        }
    }

    // protected: insert new element to the map
    pub fn insert(&mut self, item: &str) {
        let index = (1 + self.v.len()) as u32;
        self.insert_as(item, index);
    }

    // insert with known index
    pub fn insert_as(&mut self, item: &str, index: u32) {
        self.v.insert(index, item.to_string());
        self.k.insert(item.to_string(), index);
    }

    /// creates dictionary from the slice of strings (useful for tests)
    pub fn from_strings(input: Vec<&str>) -> Self {
        let mut out = Self::new();
        for item in input {
            out.insert(item);
        }
        return out;
    }
    /// creates dictionary from the slice of strings (useful for tests)
    pub fn from_static(input: &[&str]) -> Self {
        let mut out = Self::new();
        for item in input {
            out.insert(item);
        }
        return out;
    }

    /// learn from json value
    pub fn learn(&mut self, input: &Value) {
        match input {
            Value::Array(value) => {
                for v in value {
                    self.learn(v);
                }
            }
            Value::Object(value) => {
                for (k, v) in value {
                    if let None = self.find_str(k.as_str()) {
                        self.insert(k.as_str());
                    };
                    self.learn(v);
                }
            }
            _ => {}
        };
    }

    /// save into writer stream
    pub fn write<W: Write>(&self, w: &mut W) -> anyhow::Result<()> {
        for (k, v) in &self.v {
            w.write_fmt(format_args!("{}: {:?}\n", k, v))
                .context("dict write")?;
        }
        Ok(())
    }

    /// from reader stream (small files only)
    pub fn from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        let lines = std::io::BufReader::new(r).lines();
        let mut out = Self::new();
        for next_line in lines {
            if let Ok(line) = next_line {
                let ln = line.trim();
                if ln.len() > 0 && !ln.starts_with("#") {
                    if let Some((k, v)) = split_at_colon(ln) {
                        let index = k.parse::<u32>().context("invalid integer")?;
                        out.insert_as(v, index);
                    }
                }
            }
        }
        Ok(out)
    }
}

impl DictionaryRead for MapDictionary {
    fn get(&self, index: u32) -> Option<&[u8]> {
        self.v.get(&index).map(|x| x.as_str().as_bytes())
    }
    fn find_str(&self, value: &str) -> Option<u32> {
        self.k.get(value).map(|x| *x)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json::Value;
    use std::env;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};
    use std::str::FromStr;

    #[test]
    pub fn it_learns_from_file() {
        if let Ok(path) = env::var("JSONDP_TRAINING_JSON") {
            if let Ok(file) = File::open(&path) {
                let reader = BufReader::new(file);
                // Read the JSON contents of the file as an instance of `User`.
                let v = serde_json::from_reader(reader).unwrap();
                let mut dict = MapDictionary::new();
                dict.learn(&v);
                println!("{:?}", dict.k);
            }
        }
    }

    #[test]
    pub fn it_learns() {
        let mut d = MapDictionary::from_strings(vec!["alpha", "beta", "gamma", "delta"]);
        let v = Value::from_str("{\"gamma\": 1, \"epsilon\": \"alpha\", \"omega\": {\"zeta\": 2}}")
            .unwrap();
        d.learn(&v);
        assert_eq!(d.k.len(), 7);
        assert_eq!(d.v.len(), 7);
    }

    #[test]
    pub fn it_writes_and_reads() {
        let d = MapDictionary::from_strings(vec!["alpha", "beta", "gamma", "delta"]);
        assert_eq!(d.k.len(), 4);
        assert_eq!(d.v.len(), 4);
        let mut buf = BufWriter::new(Vec::new());
        d.write(&mut buf).unwrap();
        let string = String::from_utf8(buf.into_inner().unwrap()).unwrap();
        let lines: Vec<&str> = string.lines().collect();
        assert_eq!(lines.len(), 4);
        let mut bufr = BufReader::new(string.as_bytes());
        let d2 = MapDictionary::from(&mut bufr).unwrap();
        assert_eq!(d2.k.len(), 4);
        assert_eq!(d2.v.len(), 4);
    }
}
