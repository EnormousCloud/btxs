mod error;
mod param;

use crate::error::Error;
use ethers::types::H256;
use param::Params;
use serde::Serialize;
use std::time::Duration;
use tracing::*;

#[derive(Debug, Clone, Serialize)]
pub struct RpcSingleRequest {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: Params,
}

pub fn get_block(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("b{:?}", hash),
        method: "eth_getBlockByHash".to_string(),
        params: Params::Array(vec![tx, true.into()]),
    }
}

pub fn get_transaction(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("x{:?}", hash),
        method: "eth_getTransactionByHash".to_string(),
        params: Params::Array(vec![tx]),
    }
}

pub fn get_receipt(hash: H256) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("r{:?}", hash),
        method: "eth_getTransactionReceipt".to_string(),
        params: Params::Array(vec![tx]),
    }
}

#[derive(Debug, Clone)]
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
    use std::env;
    use std::str::FromStr;

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
