use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::io::{self, BufRead, Write};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const DEFAULT_SOCKET_PATH: &str = "/tmp/wicket.sock";

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    arguments: Value,
}

#[derive(Debug, Serialize)]
struct ApprovalRequest {
    tool_name: String,
    input: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_use_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApprovalResponse {
    behavior: String,
    #[serde(default)]
    message: Option<String>,
}

fn socket_path() -> String {
    env::var("WICKET_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
}

async fn request_approval(
    tool_name: String,
    input: Value,
    tool_use_id: Option<String>,
) -> Result<Value, String> {
    let path = socket_path();

    let stream = match UnixStream::connect(&path).await {
        Ok(s) => s,
        Err(e) => {
            return Ok(json!({
                "behavior": "deny",
                "message": format!("No approval interface connected ({})", e)
            }));
        }
    };

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let original_input = input.clone();
    let request = ApprovalRequest {
        tool_name,
        input,
        tool_use_id,
    };

    let mut request_json = serde_json::to_string(&request).map_err(|e| e.to_string())?;
    request_json.push('\n');

    writer
        .write_all(request_json.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    writer.flush().await.map_err(|e| e.to_string())?;

    let mut response_line = String::new();
    reader
        .read_line(&mut response_line)
        .await
        .map_err(|e| e.to_string())?;

    let response: ApprovalResponse =
        serde_json::from_str(&response_line).map_err(|e| e.to_string())?;

    if response.behavior == "allow" {
        Ok(json!({ "behavior": "allow", "updatedInput": original_input }))
    } else {
        Ok(json!({
            "behavior": "deny",
            "message": response.message.unwrap_or_else(|| "User denied permission".to_string())
        }))
    }
}

fn handle_initialize(id: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "wicket",
                "version": "0.1.0"
            }
        })),
        error: None,
    }
}

fn handle_tools_list(id: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(json!({
            "tools": [{
                "name": "wicket_approve",
                "description": "Request human approval for a tool call",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tool_name": {
                            "type": "string",
                            "description": "Name of the tool requesting approval"
                        },
                        "input": {
                            "type": "object",
                            "description": "Input arguments for the tool"
                        },
                        "tool_use_id": {
                            "type": "string",
                            "description": "Optional tool use ID"
                        }
                    },
                    "required": ["tool_name", "input"]
                }
            }]
        })),
        error: None,
    }
}

async fn handle_tools_call(id: Value, params: Value) -> JsonRpcResponse {
    let tool_params: ToolCallParams = match serde_json::from_value(params) {
        Ok(p) => p,
        Err(e) => {
            return JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32602,
                    message: format!("Invalid params: {}", e),
                }),
            };
        }
    };

    if tool_params.name != "wicket_approve" {
        return JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: format!("Unknown tool: {}", tool_params.name),
            }),
        };
    }

    let tool_name = tool_params.arguments["tool_name"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let input = tool_params.arguments["input"].clone();
    let tool_use_id = tool_params.arguments["tool_use_id"]
        .as_str()
        .map(|s| s.to_string());

    match request_approval(tool_name, input, tool_use_id).await {
        Ok(response) => {
            let response_text = serde_json::to_string(&response).unwrap();
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(json!({
                    "content": [{
                        "type": "text",
                        "text": response_text
                    }]
                })),
                error: None,
            }
        }
        Err(e) => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code: -32000,
                message: e,
            }),
        },
    }
}

#[tokio::main]
async fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        if line.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to parse request: {}", e);
                continue;
            }
        };

        // Notifications have no id and expect no response
        if request.id.is_none() {
            continue;
        }

        let id = request.id.unwrap();

        let response = match request.method.as_str() {
            "initialize" => handle_initialize(id),
            "tools/list" => handle_tools_list(id),
            "tools/call" => handle_tools_call(id, request.params).await,
            _ => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                }),
            },
        };

        let response_json = serde_json::to_string(&response).unwrap();
        writeln!(stdout, "{}", response_json).unwrap();
        stdout.flush().unwrap();
    }
}
