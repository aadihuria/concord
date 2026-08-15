#![allow(clippy::useless_conversion)]

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use tonic::transport::Channel;

use concord_proto::concord::client_service_client::ClientServiceClient;
use concord_proto::concord::*;

#[pyclass]
struct ConcordClient {
    rt: Runtime,
    client: ClientServiceClient<Channel>,
    node_id: String,
    timestamp: std::sync::atomic::AtomicU64,
}

#[pymethods]
impl ConcordClient {
    #[new]
    #[pyo3(signature = (addr, node_id="python-client".to_string()))]
    fn new(addr: String, node_id: String) -> PyResult<Self> {
        let rt = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;

        let full_addr = if addr.starts_with("http") {
            addr.clone()
        } else {
            format!("http://{}", addr)
        };

        let client = rt.block_on(async {
            let endpoint = tonic::transport::Endpoint::from_shared(full_addr)
                .map_err(|e| PyConnectionError::new_err(format!("invalid addr: {}", e)))?
                .connect_timeout(std::time::Duration::from_secs(5));

            let channel = endpoint
                .connect()
                .await
                .map_err(|e| PyConnectionError::new_err(format!("connection failed: {}", e)))?;

            Ok::<_, PyErr>(ClientServiceClient::new(channel))
        })?;

        Ok(Self {
            rt,
            client,
            node_id,
            timestamp: std::sync::atomic::AtomicU64::new(1),
        })
    }

    fn get(&self, namespace: &str, key: &str) -> PyResult<Option<PyObject>> {
        let req = GetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .get(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        if resp.found {
            Python::with_gil(|py| {
                let val: serde_json::Value = serde_json::from_str(&resp.value_json)
                    .map_err(|e| PyValueError::new_err(format!("json parse error: {}", e)))?;
                json_to_py(py, &val).map(Some)
            })
        } else {
            Ok(None)
        }
    }

    fn put(&mut self, namespace: &str, key: &str, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let json_val = py_to_json(value)?;
        let ts = self.next_timestamp();

        let req = PutRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value_json: serde_json::to_string(&json_val)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
            node_id: self.node_id.clone(),
            timestamp: ts,
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .put(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        if !resp.success && !resp.leader_hint.is_empty() {
            return Err(PyRuntimeError::new_err(format!(
                "not leader, try: {}",
                resp.leader_hint
            )));
        }

        Ok(resp.success)
    }

    fn delete(&mut self, namespace: &str, key: &str) -> PyResult<bool> {
        let req = DeleteRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .delete(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        Ok(resp.success)
    }

    fn add_to_set(&mut self, namespace: &str, key: &str, element: &str) -> PyResult<bool> {
        let req = AddToSetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            element: element.to_string(),
            node_id: self.node_id.clone(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .add_to_set(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        Ok(resp.success)
    }

    fn remove_from_set(&mut self, namespace: &str, key: &str, element: &str) -> PyResult<bool> {
        let req = RemoveFromSetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            element: element.to_string(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .remove_from_set(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        Ok(resp.success)
    }

    fn get_set(&self, namespace: &str, key: &str) -> PyResult<Option<Vec<String>>> {
        let req = GetSetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .get_set(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        if resp.found {
            Ok(Some(resp.elements))
        } else {
            Ok(None)
        }
    }

    fn insert_into_sequence(
        &mut self,
        namespace: &str,
        key: &str,
        position: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let json_val = py_to_json(value)?;
        let ts = self.next_timestamp();

        let req = InsertIntoSequenceRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            position,
            value_json: serde_json::to_string(&json_val)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
            node_id: self.node_id.clone(),
            timestamp: ts,
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .insert_into_sequence(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        Ok(resp.success)
    }

    fn delete_from_sequence(
        &mut self,
        namespace: &str,
        key: &str,
        position: u32,
    ) -> PyResult<bool> {
        let req = DeleteFromSequenceRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            position,
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .delete_from_sequence(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        Ok(resp.success)
    }

    fn get_sequence(&self, namespace: &str, key: &str) -> PyResult<Option<Vec<PyObject>>> {
        let req = GetSequenceRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        };

        let resp = self
            .rt
            .block_on(async {
                let mut client = self.client.clone();
                client
                    .get_sequence(req)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("rpc error: {}", e)))
            })?
            .into_inner();

        if resp.found {
            Python::with_gil(|py| {
                let mut values = Vec::new();
                for v in &resp.values_json {
                    let val: serde_json::Value = serde_json::from_str(v)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    values.push(json_to_py(py, &val)?);
                }
                Ok(Some(values))
            })
        } else {
            Ok(None)
        }
    }
}

impl ConcordClient {
    fn next_timestamp(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let prev = self
            .timestamp
            .fetch_max(now, std::sync::atomic::Ordering::SeqCst);
        if now <= prev {
            self.timestamp
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        } else {
            now
        }
    }
}

fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(serde_json::json!(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else if let Ok(list) = obj.downcast::<pyo3::types::PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_json(&item)?);
        }
        Ok(serde_json::Value::Array(arr))
    } else if let Ok(dict) = obj.downcast::<pyo3::types::PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(&v)?);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        let s = obj.str()?.to_string();
        Ok(serde_json::Value::String(s))
    }
}

fn json_to_py(py: Python<'_>, val: &serde_json::Value) -> PyResult<PyObject> {
    use pyo3::ToPyObject;
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.to_object(py)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.to_object(py)),
        serde_json::Value::Array(arr) => {
            let items: Vec<PyObject> = arr
                .iter()
                .map(|item| json_to_py(py, item))
                .collect::<PyResult<_>>()?;
            Ok(pyo3::types::PyList::new_bound(py, &items)
                .into_any()
                .unbind())
        }
        serde_json::Value::Object(map) => {
            let dict = pyo3::types::PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

#[pymodule]
fn concord(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ConcordClient>()?;
    Ok(())
}
