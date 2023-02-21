mod error;
mod param;

use crate::error::Error;
use ethers::types::{Address, BlockNumber, Filter, Topic, H256};
use param::Params;
use serde::Serialize;
use std::time::Duration;
use tracing::*;

/// request to be passed to JSON-RPC as a part of the batch
#[derive(Debug, Clone, Serialize)]
pub struct RpcSingleRequest {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: Params,
}

/// request to retrieve block by hash
pub fn get_block(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("b{:?}", hash),
        method: "eth_getBlockByHash".to_string(),
        params: Params::Array(vec![tx, true.into()]),
    }
}

/// request to retrieve transaction by hash
pub fn get_transaction(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("x{:?}", hash),
        method: "eth_getTransactionByHash".to_string(),
        params: Params::Array(vec![tx]),
    }
}

/// request to retrieve transaction receipt by hash
pub fn get_receipt(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("r{:?}", hash),
        method: "eth_getTransactionReceipt".to_string(),
        params: Params::Array(vec![tx]),
    }
}

/// request to retrieve logs from smart contract
pub fn get_logs(
    addresses: Vec<Address>,
    from_block: Option<BlockNumber>,
    to: Option<BlockNumber>,
    topic0: Option<Topic>,
    topic1: Option<Topic>,
    topic2: Option<Topic>,
    topic3: Option<Topic>,
) -> RpcSingleRequest {
    let mut filter = Filter::new().address(addresses);
    if let Some(from) = &from_block {
        filter = filter.from_block(from.clone());
    }
    if let Some(to) = &to {
        filter = filter.to_block(to.clone());
    }
    if let Some(topic0) = &topic0 {
        filter = filter.topic0(topic0.clone());
    }
    if let Some(topic1) = &topic1 {
        filter = filter.topic1(topic1.clone());
    }
    if let Some(topic2) = &topic2 {
        filter = filter.topic2(topic2.clone());
    }
    if let Some(topic3) = &topic3 {
        filter = filter.topic3(topic3.clone());
    }
    let filter_json = serde_json::to_value(&filter).unwrap();
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: "l".to_string(),
        method: "eth_getLogs".to_string(),
        params: Params::Array(vec![filter_json]),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcBatchResponse(Vec<serde_json::Value>);

impl RpcBatchResponse {
    pub fn value(&self, id: &str) -> Result<serde_json::Value, Error> {
        let found = self
            .0
            .iter()
            .find(|v| v["id"] == serde_json::Value::String(id.to_string()));
        match found {
            Some(v) => {
                let out = match v.get("result") {
                    Some(v) => v.clone(),
                    None => {
                        let e = v.get("error").unwrap();
                        let s = serde_json::to_string(e).unwrap();
                        let err: Error = serde_json::from_str(&s).unwrap();
                        return Err(err);
                    }
                };
                Ok(out)
            }
            None => Err(Error::not_found()),
        }
    }
}

/// Ethereum JSON-RPC client
pub struct EthBatchClient {
    rpc_addr: String,
    agent: ureq::Agent,
}

impl EthBatchClient {
    /// creates Ethereum client instance
    pub fn new(rpc_addr: &str) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_read(Duration::from_secs(60))
            .timeout_write(Duration::from_secs(5))
            .build();
        Self {
            agent,
            rpc_addr: rpc_addr.to_string(),
        }
    }

    #[instrument(skip(self), level = "debug")]
    pub fn get(&self, requests: Vec<RpcSingleRequest>) -> anyhow::Result<RpcBatchResponse> {
        let req = self
            .agent
            .post(&self.rpc_addr)
            .set("Content-Type", "application/json");
        let body = serde_json::to_string(&requests)?;
        let response = req.send_string(&body)?;
        let response_str = response.into_string()?;
        let out: Vec<serde_json::Value> = serde_json::from_str(&response_str)?;
        Ok(RpcBatchResponse(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::H256;
    use std::env;
    use std::str::FromStr;

    #[test]
    #[ignore]
    fn it_reads_logs() {
        let rpc_addr = env::var("RPC_ETH_ADDR").unwrap();
        let addresses =
            vec![Address::from_str("0b38210ea11411557c13457d4da7dc6ea731b88a").unwrap()];
        let topic =
            H256::from_str("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
                .unwrap()
                .into();
        let rq = vec![get_logs(
            addresses,
            None,
            None,
            Some(topic),
            None,
            None,
            None,
        )];
        println!("{}", serde_json::to_string(&rq).unwrap());
        let response = EthBatchClient::new(&rpc_addr).get(rq).unwrap();
        println!("{}", serde_json::to_string_pretty(&response).unwrap());
        response.value("l").unwrap();
    }

    #[test]
    #[ignore]
    fn it_reads_batch() {
        let rpc_addr = env::var("RPC_ETH_ADDR").unwrap();
        let block_id =
            H256::from_str("6773963483ac8af3c8e1e65e48a4c8eeb272f56b10534ae5356795415f817a74")
                .unwrap();
        let tx_id =
            H256::from_str("2d8a0041b55fb5d76e69b195fbbec1022133a8f09af7168a8617b270b6ef3bec")
                .unwrap();
        let rq = vec![
            get_block(block_id),
            get_transaction(tx_id),
            get_receipt(tx_id),
        ];
        println!("{}", serde_json::to_string(&rq).unwrap());
        let response = EthBatchClient::new(&rpc_addr).get(rq).unwrap();
        println!("{:?}", response);
        response.value(&format!("b{:?}", block_id)).unwrap();
        response.value(&format!("x{:?}", tx_id)).unwrap();
        response.value(&format!("r{:?}", tx_id)).unwrap();
    }
}
