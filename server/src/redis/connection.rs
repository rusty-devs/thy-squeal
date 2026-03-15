use super::resp::{RespValue, read_value};
use crate::sql::executor::Executor;
use crate::storage::Value;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{debug, error};

fn extract_bulk_string(v: &RespValue) -> Result<String> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).to_string()),
        RespValue::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow!("Expected bulk string")),
    }
}

fn extract_value(v: &RespValue) -> Result<Value> {
    match v {
        RespValue::BulkString(Some(b)) => Ok(Value::Text(String::from_utf8_lossy(b).to_string())),
        RespValue::SimpleString(s) => Ok(Value::Text(s.clone())),
        RespValue::Integer(i) => Ok(Value::Int(*i)),
        _ => Err(anyhow!("Invalid value type")),
    }
}

fn extract_integer(v: &RespValue) -> Result<i64> {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .parse()
            .map_err(|e| anyhow!("{}", e)),
        RespValue::SimpleString(s) => s.parse().map_err(|e| anyhow!("{}", e)),
        RespValue::Integer(i) => Ok(*i),
        _ => Err(anyhow!("Expected integer")),
    }
}

fn extract_float(v: &RespValue) -> Result<f64> {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b)
            .parse()
            .map_err(|e| anyhow!("{}", e)),
        RespValue::SimpleString(s) => s.parse().map_err(|e| anyhow!("{}", e)),
        RespValue::Integer(i) => Ok(*i as f64),
        _ => Err(anyhow!("Expected number")),
    }
}

pub async fn handle_connection(mut socket: TcpStream, executor: Arc<Executor>) -> Result<()> {
    loop {
        let value = match read_value(&mut socket).await {
            Ok(v) => v,
            Err(e) => {
                if e.to_string().contains("early eof") || e.to_string().contains("broken pipe") {
                    break;
                }
                error!("Error reading RESP value: {}", e);
                break;
            }
        };

        let cmd_array = match value {
            RespValue::Array(Some(a)) => a,
            _ => {
                RespValue::Error("ERR expected array".to_string())
                    .write(&mut socket)
                    .await?;
                continue;
            }
        };

        if cmd_array.is_empty() {
            continue;
        }

        let cmd_name = match &cmd_array[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
            RespValue::SimpleString(s) => s.to_uppercase(),
            _ => {
                RespValue::Error("ERR invalid command name type".to_string())
                    .write(&mut socket)
                    .await?;
                continue;
            }
        };

        debug!("Redis command: {}", cmd_name);

        match cmd_name.as_str() {
            "PING" => {
                RespValue::SimpleString("PONG".to_string())
                    .write(&mut socket)
                    .await?;
            }
            "SET" => {
                if cmd_array.len() < 3 {
                    RespValue::Error("ERR wrong number of arguments for 'set' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let val = match &cmd_array[2] {
                    RespValue::BulkString(Some(b)) => {
                        Value::Text(String::from_utf8_lossy(b).to_string())
                    }
                    _ => {
                        RespValue::Error("ERR invalid value type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };

                executor.kv_set(key, val, None).await?;
                RespValue::SimpleString("OK".to_string())
                    .write(&mut socket)
                    .await?;
            }
            "GET" => {
                if cmd_array.len() < 2 {
                    RespValue::Error("ERR wrong number of arguments for 'get' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };

                match executor.kv_get(&key, None).await? {
                    Some(Value::Text(t)) => {
                        RespValue::BulkString(Some(t.into_bytes()))
                            .write(&mut socket)
                            .await?;
                    }
                    Some(v) => {
                        RespValue::BulkString(Some(format!("{:?}", v).into_bytes()))
                            .write(&mut socket)
                            .await?;
                    }
                    None => {
                        RespValue::BulkString(None).write(&mut socket).await?;
                    }
                }
            }
            "DEL" => {
                if cmd_array.len() < 2 {
                    RespValue::Error("ERR wrong number of arguments for 'del' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let mut count = 0;
                for item in cmd_array {
                    let key = match item {
                        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
                        _ => continue,
                    };
                    if executor.kv_get(&key, None).await?.is_some() {
                        executor.kv_del(key, None).await?;
                        count += 1;
                    }
                }
                RespValue::Integer(count).write(&mut socket).await?;
            }
            "EXISTS" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'exists' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let exists = executor.kv_exists(&key, None).await?;
                RespValue::Integer(if exists { 1 } else { 0 })
                    .write(&mut socket)
                    .await?;
            }
            "EXPIRE" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'expire' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let seconds = match &cmd_array[2] {
                    RespValue::BulkString(Some(b)) => {
                        let s = String::from_utf8_lossy(b).to_string();
                        s.parse::<u64>().map_err(|e| anyhow::anyhow!("{}", e))
                    }
                    RespValue::SimpleString(s) => {
                        s.parse::<u64>().map_err(|e| anyhow::anyhow!("{}", e))
                    }
                    RespValue::Integer(i) => Ok(*i as u64),
                    _ => Err(anyhow::anyhow!("invalid number")),
                };
                let seconds = match seconds {
                    Ok(s) => s,
                    Err(_) => {
                        RespValue::Error("ERR value is not an integer".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let result = executor.kv_expire(key, seconds, None).await?;
                RespValue::Integer(if result { 1 } else { 0 })
                    .write(&mut socket)
                    .await?;
            }
            "TTL" => {
                if cmd_array.len() < 2 {
                    RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_string())
                        .write(&mut socket)
                        .await?;
                    continue;
                }
                let key = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid key type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let ttl = executor.kv_ttl(&key, None).await?;
                RespValue::Integer(ttl).write(&mut socket).await?;
            }
            "KEYS" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'keys' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let pattern = match &cmd_array[1] {
                    RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        RespValue::Error("ERR invalid pattern type".to_string())
                            .write(&mut socket)
                            .await?;
                        continue;
                    }
                };
                let keys = executor.kv_keys(&pattern, None).await?;
                let result: Vec<RespValue> = keys
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(k.into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "HSET" | "HSETNX" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'hset' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let field = extract_bulk_string(&cmd_array[2])?;
                let value = extract_value(&cmd_array[3])?;
                executor.kv_hash_set(key, field, value, None).await?;
                RespValue::Integer(1).write(&mut socket).await?;
            }
            "HGET" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'hget' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let field = extract_bulk_string(&cmd_array[2])?;
                match executor.kv_hash_get(&key, &field, None).await? {
                    Some(v) => {
                        RespValue::BulkString(Some(format!("{:?}", v).into_bytes()))
                            .write(&mut socket)
                            .await?
                    }
                    None => RespValue::BulkString(None).write(&mut socket).await?,
                }
            }
            "HGETALL" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'hgetall' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let hash = executor.kv_hash_get_all(&key, None).await?;
                let mut result = vec![];
                for (field, value) in hash {
                    result.push(RespValue::BulkString(Some(field.into_bytes())));
                    let val_str = format!("{:?}", value);
                    result.push(RespValue::BulkString(Some(val_str.into_bytes())));
                }
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "HDEL" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'hdel' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let fields: Vec<String> = cmd_array[2..]
                    .iter()
                    .filter_map(|v| extract_bulk_string(v).ok())
                    .collect();
                let count = executor.kv_hash_del(key, fields, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "LPUSH" | "RPUSH" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'lpush/rpush' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let values: Vec<Value> = cmd_array[2..]
                    .iter()
                    .map(|v| extract_value(v).unwrap_or(Value::Null))
                    .collect();
                let left = cmd_name == "LPUSH";
                let count = executor.kv_list_push(key, values, left, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "LRANGE" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'lrange' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let start = extract_integer(&cmd_array[2])? as i64;
                let stop = extract_integer(&cmd_array[3])? as i64;
                let values = executor.kv_list_range(&key, start, stop, None).await?;
                let result: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "LPOP" | "RPOP" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'lpop/rpop' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let count = if cmd_array.len() > 2 {
                    extract_integer(&cmd_array[2])? as usize
                } else {
                    1
                };
                let left = cmd_name == "LPOP";
                let values = executor.kv_list_pop(key, count, left, None).await?;
                let result: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "LLEN" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'llen' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let len = executor.kv_list_len(&key, None).await?;
                RespValue::Integer(len as i64).write(&mut socket).await?;
            }
            "SADD" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'sadd' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let members: Vec<String> = cmd_array[2..]
                    .iter()
                    .filter_map(|v| extract_bulk_string(v).ok())
                    .collect();
                let count = executor.kv_set_add(key, members, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "SMEMBERS" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'smembers' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let members = executor.kv_set_members(&key, None).await?;
                let result: Vec<RespValue> = members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m.into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "SISMEMBER" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'sismember' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let member = extract_bulk_string(&cmd_array[2])?;
                let exists = executor.kv_set_is_member(&key, &member, None).await?;
                RespValue::Integer(if exists { 1 } else { 0 })
                    .write(&mut socket)
                    .await?;
            }
            "SREM" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'srem' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let members: Vec<String> = cmd_array[2..]
                    .iter()
                    .filter_map(|v| extract_bulk_string(v).ok())
                    .collect();
                let count = executor.kv_set_remove(key, members, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "ZADD" => {
                if cmd_array.len() < 4 || cmd_array.len() % 2 != 0 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'zadd' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let mut members = vec![];
                let mut i = 2;
                while i + 1 < cmd_array.len() {
                    let score = extract_float(&cmd_array[i])?;
                    let member = extract_bulk_string(&cmd_array[i + 1])?;
                    members.push((score, member));
                    i += 2;
                }
                let count = executor.kv_zset_add(key, members, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "ZRANGE" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'zrange' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let start = extract_integer(&cmd_array[2])? as i64;
                let stop = extract_integer(&cmd_array[3])? as i64;
                let with_scores = cmd_array.len() > 4
                    && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"WITHSCORES");
                let values = executor
                    .kv_zset_range(&key, start, stop, with_scores, None)
                    .await?;
                let result: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "ZRANGEBYSCORE" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'zrangebyscore' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let min = extract_float(&cmd_array[2])?;
                let max = extract_float(&cmd_array[3])?;
                let with_scores = cmd_array.len() > 4
                    && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"WITHSCORES");
                let values = executor
                    .kv_zsetrangebyscore(&key, min, max, with_scores, None)
                    .await?;
                let result: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(format!("{:?}", v).into_bytes())))
                    .collect();
                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "ZREM" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'zrem' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let members: Vec<String> = cmd_array[2..]
                    .iter()
                    .filter_map(|v| extract_bulk_string(v).ok())
                    .collect();
                let count = executor.kv_zset_remove(key, members, None).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "XADD" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'xadd' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;

                let id = if matches!(&cmd_array[2], RespValue::BulkString(Some(b)) if b.starts_with(b"*"))
                {
                    None
                } else if cmd_array.len() > 2 {
                    Some(extract_integer(&cmd_array[2])? as u64)
                } else {
                    None
                };

                let start_idx = if cmd_array.len() > 2
                    && matches!(&cmd_array[2], RespValue::BulkString(Some(b)) if !b.starts_with(b"*"))
                {
                    3
                } else {
                    2
                };

                let mut fields = HashMap::new();
                let mut i = start_idx;
                while i + 1 < cmd_array.len() {
                    let field = extract_bulk_string(&cmd_array[i])?;
                    let value =
                        extract_value(&cmd_array[i + 1]).unwrap_or(Value::Text("".to_string()));
                    fields.insert(field, value);
                    i += 2;
                }

                let stream_id = executor.kv_stream_add(key, id, fields, None).await?;
                RespValue::BulkString(Some(stream_id.into_bytes()))
                    .write(&mut socket)
                    .await?;
            }
            "XRANGE" => {
                if cmd_array.len() < 4 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'xrange' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let start = extract_bulk_string(&cmd_array[2])?;
                let stop = extract_bulk_string(&cmd_array[3])?;

                let count = if cmd_array.len() > 4
                    && matches!(&cmd_array[4], RespValue::BulkString(Some(b)) if b == b"COUNT")
                {
                    Some(extract_integer(&cmd_array[5])? as usize)
                } else {
                    None
                };

                let results = executor
                    .kv_stream_range(&key, &start, &stop, count, None)
                    .await?;

                let mut result = vec![];
                for (id, fields) in results {
                    let mut entry = vec![RespValue::BulkString(Some(id.into_bytes()))];
                    let mut field_values = vec![];
                    for (field, value) in fields {
                        field_values.push(RespValue::BulkString(Some(field.into_bytes())));
                        field_values.push(RespValue::BulkString(Some(
                            format!("{:?}", value).into_bytes(),
                        )));
                    }
                    entry.push(RespValue::Array(Some(field_values)));
                    result.push(RespValue::Array(Some(entry)));
                }

                RespValue::Array(Some(result)).write(&mut socket).await?;
            }
            "XLEN" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'xlen' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let key = extract_bulk_string(&cmd_array[1])?;
                let len = executor.kv_stream_len(&key, None).await?;
                RespValue::Integer(len as i64).write(&mut socket).await?;
            }
            "PUBLISH" => {
                if cmd_array.len() < 3 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'publish' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let channel = extract_bulk_string(&cmd_array[1])?;
                let message = extract_bulk_string(&cmd_array[2])?;
                let count = executor.pubsub_publish(channel, message).await?;
                RespValue::Integer(count as i64).write(&mut socket).await?;
            }
            "SUBSCRIBE" | "PSUBSCRIBE" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'subscribe' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let client_id = format!("{:p}", &socket);
                for item in cmd_array.iter().skip(1) {
                    let channel = extract_bulk_string(item)?;
                    executor
                        .pubsub_subscribe(client_id.clone(), channel.clone())
                        .await?;
                    RespValue::Array(Some(vec![
                        RespValue::SimpleString("subscribe".to_string()),
                        RespValue::BulkString(Some(channel.into_bytes())),
                        RespValue::Integer(1),
                    ]))
                    .write(&mut socket)
                    .await?;
                }
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                let client_id = format!("{:p}", &socket);
                if cmd_array.len() < 2 {
                    executor.pubsub_unsubscribe(client_id, None).await?;
                } else {
                    for item in cmd_array.iter().skip(1) {
                        let channel = extract_bulk_string(item)?;
                        executor
                            .pubsub_unsubscribe(client_id.clone(), Some(channel))
                            .await?;
                    }
                }
                RespValue::Array(Some(vec![
                    RespValue::SimpleString("unsubscribe".to_string()),
                    RespValue::BulkString(None),
                    RespValue::Integer(0),
                ]))
                .write(&mut socket)
                .await?;
            }
            "PUBSUB" => {
                if cmd_array.len() < 2 {
                    RespValue::Error(
                        "ERR wrong number of arguments for 'pubsub' command".to_string(),
                    )
                    .write(&mut socket)
                    .await?;
                    continue;
                }
                let subcommand = extract_bulk_string(&cmd_array[1])?.to_uppercase();
                match subcommand.as_str() {
                    "CHANNELS" => {
                        let channels = executor.pubsub_channels().await?;
                        let result: Vec<RespValue> = channels
                            .into_iter()
                            .map(|c| RespValue::BulkString(Some(c.into_bytes())))
                            .collect();
                        RespValue::Array(Some(result)).write(&mut socket).await?;
                    }
                    "NUMSUB" => {
                        RespValue::Integer(0).write(&mut socket).await?;
                    }
                    _ => {
                        RespValue::Error("ERR Unknown PUBSUB subcommand".to_string())
                            .write(&mut socket)
                            .await?;
                    }
                }
            }
            "QUIT" => {
                RespValue::SimpleString("OK".to_string())
                    .write(&mut socket)
                    .await?;
                break;
            }
            _ => {
                RespValue::Error(format!("ERR unknown command '{}'", cmd_name))
                    .write(&mut socket)
                    .await?;
            }
        }
    }
    Ok(())
}
