mod error;
mod param;

use crate::error::{Error, ErrorContainer};
use anyhow::{bail, Context};
use ethers::types::{
    Address, Block, BlockNumber, Filter, Log, Topic, Transaction, TransactionReceipt, TxHash, H256,
};
use param::Params;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap as Map;
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

/// request to retrieve latest block number
pub fn get_latest() -> RpcSingleRequest {
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: "latest".to_string(),
        method: "eth_blockNumber".to_string(),
        params: Params::Array(vec![]),
    }
}

/// request to rerieve chain id from network_version
pub fn get_net_version() -> RpcSingleRequest {
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: "net".to_string(),
        method: "net_version".to_string(),
        params: Params::Array(vec![]),
    }
}

/// request to retrieve block by hash
pub fn get_block(hash: H256, transactions: bool) -> RpcSingleRequest {
    let tx = serde_json::Value::String(format!("{:?}", &hash));
    RpcSingleRequest {
        jsonrpc: "2.0".to_string(),
        id: format!("b{:?}", hash),
        method: "eth_getBlockByHash".to_string(),
        params: Params::Array(vec![tx, transactions.into()]),
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
    to_block: Option<BlockNumber>,
    topic0: Option<Topic>,
    topic1: Option<Topic>,
    topic2: Option<Topic>,
    topic3: Option<Topic>,
) -> RpcSingleRequest {
    let mut filter = Filter::new().address(addresses);
    if let Some(from) = &from_block {
        filter = filter.from_block(from.clone());
    }
    if let Some(to) = &to_block {
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
    pub fn value(&self, id: &str) -> Result<Value, Error> {
        let found = self
            .0
            .iter()
            .find(|v| v["id"] == Value::String(id.to_string()));
        match found {
            Some(v) => {
                let out = match v.get("result") {
                    Some(v) => v.clone(),
                    None => {
                        if let Some(e) = v.get("error") {
                            let err: Error = serde_json::from_value(e.clone()).unwrap();
                            return Err(err);
                        } else {
                            println!("no error, no result");
                            return Err(Error::not_found());
                        }
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
        // check if the response is just a single error
        if let Ok(err) = serde_json::from_str::<ErrorContainer>(&response_str) {
            return Err(err.error.into());
        }
        let out: Vec<serde_json::Value> = serde_json::from_str(&response_str)?;
        Ok(RpcBatchResponse(out))
    }

    /// try out connection to RPC and return chain id and latest block number if successful
    #[instrument(skip(self), level = "debug")]
    pub fn connect(&self) -> anyhow::Result<(u64, u64)> {
        let our = self.get(vec![get_net_version(), get_latest()])?;
        let chain_id = match our.value("net")? {
            Value::Number(n) => n.as_u64().context("failed to parse chain_id as number")?,
            Value::String(s) => {
                if s.starts_with("0x") {
                    u64::from_str_radix(&s[2..], 16).context("failed to parse chain_id as hex")?
                } else {
                    s.parse().context("failed to parse chain_id as decimal")?
                }
            }
            _ => bail!("net_version is neither number or string"),
        };
        let block_id = match our.value("latest")? {
            Value::Number(n) => n
                .as_u64()
                .context("failed to parse latest block as number")?,
            Value::String(s) => {
                if s.starts_with("0x") {
                    u64::from_str_radix(&s[2..], 16)
                        .context("failed to parse latest block as hex")?
                } else {
                    s.parse()
                        .context("failed to parse latest block as decimal")?
                }
            }
            _ => bail!("latest block is neither number or string"),
        };
        Ok((chain_id, block_id))
    }
}

pub struct BlockTransactions {
    pub block: Block<TxHash>,
    pub transactions: Vec<Transaction>,
    pub receipts: Map<TxHash, TransactionReceipt>,
}

pub struct EthLogsStream {
    client: EthBatchClient,
    latest_event_block: u64,
    latest_block: u64,
    batch_size: u64,
    addresses: Vec<Address>,
    topic0: Option<Topic>,
    topic1: Option<Topic>,
    topic2: Option<Topic>,
    topic3: Option<Topic>,
}

impl EthLogsStream {
    // create stream
    pub fn new(
        client: EthBatchClient,
        min_block: u64,
        batch_size: u64,
        addresses: Vec<Address>,
        topic0: Option<Topic>,
        topic1: Option<Topic>,
        topic2: Option<Topic>,
        topic3: Option<Topic>,
    ) -> anyhow::Result<Self> {
        let (_, latest_block) = client.connect()?;
        // TODO: pick up the latest events from the KV storage
        let latest_event_block = min_block - 1;
        Ok(Self {
            client,
            latest_event_block,
            latest_block,
            batch_size,
            addresses,
            topic0,
            topic1,
            topic2,
            topic3,
        })
    }

    pub fn next(&self) -> anyhow::Result<Option<BlockTransactions>> {
        let mut current_block = self.latest_event_block;
        while current_block < self.latest_block {
            let to_block = std::cmp::min(current_block + self.batch_size, self.latest_block);
            // 1st request download the logs
            let requests = vec![get_logs(
                self.addresses.clone(),
                Some(current_block.into()),
                Some(to_block.into()),
                self.topic0.clone(),
                self.topic1.clone(),
                self.topic2.clone(),
                self.topic3.clone(),
            )];
            println!("request: {:?}", requests);
            let response = self.client.get(requests)?;
            let logs: Vec<Log> = serde_json::from_value(response.value("logs")?)?;

            let mut bm = Map::<H256, Block<TxHash>>::new();
            for l in logs {
                let blockHash = l.block_hash.context("no block hash")?;
                // download block by its hash, it its not there already
                if !bm.contains_key(&blockHash) {
                    let requests = vec![get_block(blockHash, false)];
                    let response = self.client.get(requests)?;
                    let block: Block<TxHash> = serde_json::from_value(response.value("block")?)?;
                    bm.insert(blockHash, block);
                }
            }
            // 2nds request:: get transactions and receipts, block by block
            let mut txs = Vec::<Transaction>::new();
            let mut receipts = Map::<TxHash, TransactionReceipt>::new();
            for (_, block) in bm.iter() {
                for tx in block.transactions.iter() {
                    let hash = &tx.clone();
                    let requests = vec![get_transaction(hash), get_receipt(hash)];
                    let response = self.client.get(requests)?;
                    let tx: Transaction = serde_json::from_value(response.value("transaction")?)?;
                    let receipt: TransactionReceipt =
                        serde_json::from_value(response.value("receipt")?)?;
                    txs.push(tx);
                    receipts.insert(tx.hash, receipt);
                }
                current_block = block.number.as_u64();
            }
        }
        Ok(None)
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
        let rpc_addr = env::var("RPC_ETH_ADDR").expect("RPC_ETH_ADDR must be set");
        let client = EthBatchClient::new(&rpc_addr);
        let (chain_id, block_id) = client.connect().unwrap();
        assert!(block_id > 17600000);
        assert_eq!(chain_id, 1);

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
        let response = client.get(rq).unwrap();
        println!("{}", serde_json::to_string_pretty(&response).unwrap());
        response.value("l").unwrap();
    }

    #[test]
    #[ignore]
    fn it_reads_batch() {
        let rpc_addr = env::var("RPC_ETH_ADDR").expect("RPC_ETH_ADDR not set");
        let client = EthBatchClient::new(&rpc_addr);
        let (chain_id, block_id) = client.connect().unwrap();
        assert_eq!(chain_id, 1);
        assert!(block_id > 17600000);

        let block_id =
            H256::from_str("6773963483ac8af3c8e1e65e48a4c8eeb272f56b10534ae5356795415f817a74")
                .unwrap();
        let tx_id =
            H256::from_str("2d8a0041b55fb5d76e69b195fbbec1022133a8f09af7168a8617b270b6ef3bec")
                .unwrap();
        let rq = vec![
            get_block(block_id, false),
            get_transaction(tx_id),
            get_receipt(tx_id),
        ];
        println!("{}", serde_json::to_string(&rq).unwrap());
        let response = client.get(rq).unwrap();
        println!("{:?}", response);
        response.value(&format!("b{:?}", block_id)).unwrap();
        response.value(&format!("x{:?}", tx_id)).unwrap();
        response.value(&format!("r{:?}", tx_id)).unwrap();
    }
}
