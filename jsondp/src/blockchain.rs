use lazy_static::lazy_static;
use std::collections::BTreeMap;

lazy_static! {
    static ref DICTIONARY: BTreeMap<&'static str, u32> = {
        let mut m = BTreeMap::new();
        m.insert("accessList", 44);
        m.insert("address", 53);
        m.insert("baseFeePerGas", 11);
        m.insert("blockHash", 30);
        m.insert("blockNumber", 31);
        m.insert("chainId", 32);
        m.insert("contractAddress", 49);
        m.insert("cumulativeGasUsed", 50);
        m.insert("currentBlock", 4);
        m.insert("data", 54);
        m.insert("difficulty", 12);
        m.insert("effectiveGasPrice", 51);
        m.insert("extraData", 13);
        m.insert("from", 33);
        m.insert("gas", 34);
        m.insert("gasLimit", 14);
        m.insert("gasPrice", 35);
        m.insert("gasUsed", 15);
        m.insert("hash", 16);
        m.insert("highestBlock", 5);
        m.insert("id", 1);
        m.insert("input", 36);
        m.insert("jsonrpc", 2);
        m.insert("knownStates", 9);
        m.insert("logIndex", 55);
        m.insert("logs", 52);
        m.insert("logsBloom", 17);
        m.insert("maxFeePerGas", 45);
        m.insert("maxPriorityFeePerGas", 46);
        m.insert("miner", 18);
        m.insert("mixHash", 19);
        m.insert("nonce", 20);
        m.insert("number", 21);
        m.insert("parentHash", 22);
        m.insert("pulledStates", 10);
        m.insert("r", 37);
        m.insert("receiptsRoot", 23);
        m.insert("removed", 56);
        m.insert("result", 3);
        m.insert("s", 38);
        m.insert("sha3Uncles", 24);
        m.insert("size", 25);
        m.insert("startingBlock", 6);
        m.insert("stateRoot", 26);
        m.insert("status", 59);
        m.insert("timestamp", 27);
        m.insert("to", 39);
        m.insert("topics", 57);
        m.insert("totalDifficulty", 28);
        m.insert("transactionHash", 58);
        m.insert("transactionIndex", 40);
        m.insert("transactions", 29);
        m.insert("transactionsRoot", 47);
        m.insert("type", 41);
        m.insert("uncles", 48);
        m.insert("v", 42);
        m.insert("value", 43);
        m.insert("warpChunksAmount", 7);
        m.insert("warpChunksProcessed", 8);
        m
    };
}

use super::MapDictionary;

pub fn get_dictionary() -> MapDictionary {
    let mut out = MapDictionary::new();
    for (k, v) in DICTIONARY.iter() {
        out.insert_as(k, *v);
    }
    out
}
