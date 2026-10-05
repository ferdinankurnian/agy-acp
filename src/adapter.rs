use fs2::FileExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use uuid::Uuid;

use crate::streaming::StreamProcessor;
use crate::types::*;

pub struct Adapter {
    pub sessions: HashMap<String, Session>,
    pub working_dir: String,
    pub state_file: PathBuf,
    pub available_models: Vec<String>,
    pub skip_naration: bool,
}

impl Adapter {
    pub const MODEL_CONFIG_ID: &'static str = "model";
    pub const EFFORT_CONFIG_ID: &'static str = "effort";
    /// Reasoning effort slugs, in canonical order. Must match `agy --effort`.
    pub const EFFORTS: [&'static str; 5] = ["low", "medium", "high", "xhigh", "max"];

    pub fn new() -> Self {
        Self::new_with_skip_naration(false)
    }

    pub fn new_with_skip_naration(skip_naration: bool) -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let state_dir = PathBuf::from(&home).join(".openab/agy-acp");
        Self {
            sessions: HashMap::new(),
            working_dir: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/tmp".to_string()),
            state_file: state_dir.join("sessions.json"),
            available_models: Self::fetch_available_models(),
            skip_naration,
        }
    }

    /// Run `agy models` and parse the output into a list of model names.
    fn fetch_available_models() -> Vec<String> {
        std::process::Command::new("agy")
            .arg("models")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| parse_available_models(&String::from_utf8_lossy(&o.stdout)))
            .unwrap_or_default()
    }

    /// Build the ACP `models` JSON for a session, given its current model_id.
    ///
    /// Effort-suffixed variants ("Gemini 3.8 Flash (High)") are collapsed to
    /// their base model ("Gemini 3.8 Flash"); effort is exposed separately via
    /// [`Self::session_config_options_json`].
    pub fn session_models_json(&mut self, model_id: Option<&str>) -> Value {
        if self.available_models.is_empty() {
            self.available_models = Self::fetch_available_models();
        }
        let (base, _) = self.resolve_selection(model_id);
        let current = if base.is_empty() {
            self.base_models().first().cloned().unwrap_or_default()
        } else {
            base
        };
        let available: Vec<Value> = self
            .base_models()
            .into_iter()
            .map(|name| {
                json!({
                    "modelId": name,
                    "name": name,
                })
            })
            .collect();
        json!({
            "currentModelId": current,
            "availableModels": available,
        })
    }

    /// Build the ACP session config options: a Model selector (base models
    /// only) plus a separate Effort selector for the current base model.
    pub fn session_config_options_json(
        &mut self,
        model_id: Option<&str>,
        effort: Option<&str>,
    ) -> Value {
        if self.available_models.is_empty() {
            self.available_models = Self::fetch_available_models();
        }
        let (base, stored_effort) = self.resolve_selection(model_id);
        let base = if base.is_empty() {
            self.base_models().first().cloned().unwrap_or_default()
        } else {
            base
        };
        // An explicitly passed effort wins; otherwise fall back to the stored one.
        let preferred = effort.unwrap_or(&stored_effort).to_string();
        let model_options: Vec<Value> = self
            .base_models()
            .into_iter()
            .map(|name| {
                json!({
                    "value": name,
                    "name": name,
                })
            })
            .collect();
        let efforts = self.efforts_for(&base);
        let current_effort = if efforts.contains(&preferred) {
            preferred
        } else {
            efforts.first().cloned().unwrap_or_default()
        };
        let effort_options: Vec<Value> = efforts
            .into_iter()
            .map(|e| {
                json!({
                    "value": e,
                    "name": Self::effort_display(&e),
                })
            })
            .collect();
        json!([
        {
            "id": Self::MODEL_CONFIG_ID,
            "name": "Model",
            "category": "model",
            "type": "select",
            "currentValue": base,
            "options": model_options,
        },
        {
            "id": Self::EFFORT_CONFIG_ID,
            "name": "Effort",
            "category": "effort",
            "type": "select",
            "currentValue": current_effort,
            "options": effort_options,
        }])
    }

    pub fn session_config_result_json(
        &mut self,
        session_id: &str,
        model_id: Option<&str>,
        effort: Option<&str>,
    ) -> Value {
        json!({
            "sessionId": session_id,
            "models": self.session_models_json(model_id),
            "configOptions": self.session_config_options_json(model_id, effort),
        })
    }

    /// Split a variant display name into (base, effort slug).
    ///
    /// "Gemini 3.8 Flash (High)" -> ("Gemini 3.8 Flash", "high").
    /// Names without a known effort suffix keep ("name", "").
    pub fn split_variant(display: &str) -> (String, String) {
        let trimmed = display.trim();
        if let Some(open) = trimmed.rfind(" (") {
            if trimmed.ends_with(')') {
                let suffix = trimmed[open + 2..trimmed.len() - 1].to_lowercase();
                if Self::EFFORTS.contains(&suffix.as_str()) {
                    return (trimmed[..open].trim().to_string(), suffix);
                }
            }
        }
        (trimmed.to_string(), String::new())
    }

    /// Distinct base model display names, in first-seen order.
    pub fn base_models(&self) -> Vec<String> {
        let mut bases = Vec::new();
        for variant in &self.available_models {
            let (base, _) = Self::split_variant(variant);
            if !bases.contains(&base) {
                bases.push(base);
            }
        }
        bases
    }

    /// Effort slugs available for a base model, in canonical order.
    /// Returns [""] (single Default choice) when the base has no effort variants.
    pub fn efforts_for(&self, base: &str) -> Vec<String> {
        let mut found = Vec::new();
        for variant in &self.available_models {
            let (b, effort) = Self::split_variant(variant);
            if b == base && !effort.is_empty() && !found.contains(&effort) {
                found.push(effort);
            }
        }
        if found.is_empty() {
            return vec![String::new()];
        }
        let mut ordered: Vec<String> = Self::EFFORTS
            .iter()
            .filter(|e| found.contains(&e.to_string()))
            .map(|e| e.to_string())
            .collect();
        // Keep any unexpected effort labels at the end, in first-seen order.
        for e in found {
            if !ordered.contains(&e) {
                ordered.push(e);
            }
        }
        ordered
    }

    /// Human label for an effort slug ("" renders as Default).
    pub fn effort_display(effort: &str) -> String {
        if effort.is_empty() {
            return "Default".to_string();
        }
        let mut chars = effort.chars();
        match chars.next() {
            None => "Default".to_string(),
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        }
    }

    /// Normalize a stored/selected model id into (base, effort).
    ///
    /// Accepts both full variant names ("Gemini 3.8 Flash (High)", including
    /// selections persisted before the effort split) and bare base names.
    pub fn resolve_selection(&self, model_id: Option<&str>) -> (String, String) {
        let Some(raw) = model_id.map(str::trim).filter(|s| !s.is_empty()) else {
            return (String::new(), String::new());
        };
        if self.available_models.iter().any(|v| v == raw) {
            return Self::split_variant(raw);
        }
        let base = raw.to_string();
        let effort = self
            .efforts_for(&base)
            .first()
            .cloned()
            .unwrap_or_default();
        (base, effort)
    }

    /// Full variant display name to pass to `agy --model` for a (base, effort).
    pub fn variant_for(&self, base: &str, effort: &str) -> String {
        for variant in &self.available_models {
            let (b, e) = Self::split_variant(variant);
            if b == base && e == effort {
                return variant.clone();
            }
        }
        if self.available_models.iter().any(|v| v == base) {
            return base.to_string();
        }
        for variant in &self.available_models {
            let (b, _) = Self::split_variant(variant);
            if b == base {
                return variant.clone();
            }
        }
        base.to_string()
    }

    /// Acquire exclusive lock on a dedicated lock file for read-write mutual exclusion.
    fn lock_state_file(&self) -> Option<fs::File> {
        if let Some(parent) = self.state_file.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let lock_path = self.state_file.with_extension("lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .ok()?;
        lock_file.lock_exclusive().ok()?;
        Some(lock_file)
    }

    /// Load persisted session store (caller must hold lock).
    fn load_store_inner(&self) -> SessionStore {
        let Some(file) = fs::File::open(&self.state_file).ok() else {
            return SessionStore::default();
        };
        serde_json::from_reader(&file).unwrap_or_default()
    }

    /// Load persisted session store with lock.
    pub fn load_store(&self) -> SessionStore {
        let _lock = self.lock_state_file();
        self.load_store_inner()
    }

    /// Try to restore conversation_id, last_step_idx, model base and effort from
    /// persisted state.
    pub fn restore_session(
        &self,
        session_id: &str,
    ) -> Option<(String, i64, Option<String>, Option<String>)> {
        let store = self.load_store();
        store.sessions.get(session_id).and_then(|s| {
            s.conversation_id.clone().map(|cid| {
                (
                    cid,
                    s.last_step_idx,
                    s.model_id.clone(),
                    s.effort.clone(),
                )
            })
        })
    }

    /// Persist a session binding (read-modify-write under single lock).
    pub fn persist_session(
        &self,
        session_id: &str,
        conversation_id: Option<&str>,
        last_step_idx: i64,
        model_id: Option<&str>,
        effort: Option<&str>,
    ) {
        let Some(_lock) = self.lock_state_file() else {
            return;
        };
        let mut store = self.load_store_inner();
        store.sessions.insert(
            session_id.to_string(),
            StoredSession {
                conversation_id: conversation_id.map(String::from),
                last_step_idx,
                model_id: model_id.map(String::from),
                effort: effort.map(String::from),
            },
        );
        let tmp = self.state_file.with_extension("tmp");
        if let Ok(file) = fs::File::create(&tmp) {
            if serde_json::to_writer_pretty(&file, &store).is_ok() {
                let _ = fs::rename(&tmp, &self.state_file);
            }
        }
    }

    /// Filter out leading narration ("I will ...", "I'll ...") from response parts.
    #[cfg(test)]
    pub fn filter_narration(parts: &[String]) -> Option<String> {
        filter_narration(parts)
    }

    /// A part is considered narration if every non-empty line starts with "I will" or "I'll".
    #[cfg(test)]
    pub fn is_narration(text: &str) -> bool {
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            return false;
        }
        lines.iter().all(|l| {
            let line = l.trim_start();
            line.starts_with("I will") || line.starts_with("I'll") || line.starts_with("I’ll")
        })
    }

    fn evict_if_needed(&mut self) {
        const MAX_SESSIONS: usize = 64;
        while self.sessions.len() >= MAX_SESSIONS {
            if let Some(key) = self.sessions.keys().next().cloned() {
                self.sessions.remove(&key);
            }
        }
    }

    pub fn restore_session_state(&mut self, session_id: &str) -> bool {
        let Some((conversation_id, last_step_idx, model_id, effort)) =
            self.restore_session(session_id)
        else {
            return false;
        };
        if !self.sessions.contains_key(session_id) {
            self.evict_if_needed();
        }
        self.sessions.insert(
            session_id.to_string(),
            Session {
                conversation_id: Some(conversation_id),
                last_step_idx,
                model_id,
                effort,
            },
        );
        true
    }

    pub fn handle_initialize(&self, id: Value) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({
                "protocolVersion": 1,
                "agentInfo": { "name": "agy", "version": env!("CARGO_PKG_VERSION") },
                "agentCapabilities": {
                    "loadSession": true,
                    "sessionCapabilities": { "resume": {} },
                },
                "authMethods": [],
            })),
            error: None,
        }
    }

    pub fn handle_session_new(&mut self, id: Value) -> JsonRpcResponse {
        let session_id = Uuid::new_v4().to_string();
        self.evict_if_needed();
        self.sessions.insert(
            session_id.clone(),
            Session {
                conversation_id: None,
                last_step_idx: -1,
                model_id: None,
                effort: None,
            },
        );
        let result = self.session_config_result_json(&session_id, None, None);
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn handle_session_load(&mut self, id: Value, params: &Value) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if session_id.is_empty() {
            return vec![serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            })
            .unwrap()];
        }

        if !self.sessions.contains_key(session_id) && !self.restore_session_state(session_id) {
            return vec![serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            })
            .unwrap()];
        }

        vec![{
            let session = self.sessions.get(session_id);
            let model_id = session.and_then(|s| s.model_id.clone());
            let effort = session.and_then(|s| s.effort.clone());
            let result =
                self.session_config_result_json(session_id, model_id.as_deref(), effort.as_deref());
            serde_json::to_string(&JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            })
            .unwrap()
        }]
    }

    pub fn handle_session_resume(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if session_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId"})),
            };
        }

        if self.sessions.contains_key(session_id) || self.restore_session_state(session_id) {
            let session = self.sessions.get(session_id);
            let model_id = session.and_then(|s| s.model_id.clone());
            let effort = session.and_then(|s| s.effort.clone());
            let result =
                self.session_config_result_json(session_id, model_id.as_deref(), effort.as_deref());
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            };
        }

        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(json!({
                "code": -32000,
                "message": format!("unknown sessionId: {session_id}"),
            })),
        }
    }

    pub fn handle_session_set_model(&mut self, id: Value, params: &Value) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let model_id = params.get("modelId").and_then(|v| v.as_str()).unwrap_or("");

        if session_id.is_empty() || model_id.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({"code":-32602,"message":"missing sessionId or modelId"})),
            };
        }

        if !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        // Accept both base names and legacy full variant names; keep the
        // current effort when it is valid for the new base. Reads happen
        // before the mutable borrow below.
        let (base, variant_effort) = self.resolve_selection(Some(model_id));
        let current_effort = self
            .sessions
            .get(session_id)
            .and_then(|s| s.effort.clone())
            .unwrap_or_default();
        let efforts = self.efforts_for(&base);
        let effort = if efforts.contains(&current_effort) {
            current_effort
        } else if efforts.contains(&variant_effort) {
            variant_effort
        } else {
            efforts.first().cloned().unwrap_or_default()
        };

        let Some(session) = self.sessions.get_mut(session_id) else {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            };
        };

        session.model_id = Some(base.clone());
        session.effort = Some(effort.clone());
        let last_step_idx = session.last_step_idx;
        let conv_id = session.conversation_id.clone();

        self.persist_session(
            session_id,
            conv_id.as_deref(),
            last_step_idx,
            Some(&base),
            Some(&effort),
        );

        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({})),
            error: None,
        }
    }

    pub fn handle_session_set_config_option(
        &mut self,
        id: Value,
        params: &Value,
    ) -> JsonRpcResponse {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let config_id = params
            .get("configId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let value = params.get("value").and_then(|v| v.as_str()).unwrap_or("");

        if session_id.is_empty() || config_id.is_empty() || value.is_empty() {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(
                    json!({"code":-32602,"message":"missing sessionId, configId, or value"}),
                ),
            };
        }

        if config_id != Self::MODEL_CONFIG_ID && config_id != Self::EFFORT_CONFIG_ID {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32602,
                    "message": format!("unknown configId: {config_id}"),
                })),
            };
        }

        if !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        // Normalize against the model list before taking a mutable borrow.
        let (base, variant_effort) = self.resolve_selection(Some(value));
        let current = self
            .sessions
            .get(session_id)
            .map(|s| {
                (
                    s.model_id.clone().unwrap_or_default(),
                    s.effort.clone().unwrap_or_default(),
                )
            })
            .unwrap_or_default();

        let (new_base, new_effort) = if config_id == Self::EFFORT_CONFIG_ID {
            let efforts = self.efforts_for(&current.0);
            let effort = if efforts.contains(&value.to_string()) {
                value.to_string()
            } else {
                // Unknown effort for this base: keep current, or fall back to default.
                if efforts.contains(&current.1) {
                    current.1.clone()
                } else {
                    // `value` may itself be a full variant name; honor its effort
                    // when valid for the current base.
                    let (_, e) = Self::split_variant(value);
                    if efforts.contains(&e) { e } else { efforts.first().cloned().unwrap_or_default() }
                }
            };
            (current.0.clone(), effort)
        } else {
            let efforts = self.efforts_for(&base);
            let effort = if efforts.contains(&current.1) {
                current.1.clone()
            } else if efforts.contains(&variant_effort) {
                variant_effort
            } else {
                efforts.first().cloned().unwrap_or_default()
            };
            (base, effort)
        };

        let Some(session) = self.sessions.get_mut(session_id) else {
            return JsonRpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(json!({
                    "code": -32000,
                    "message": format!("unknown sessionId: {session_id}"),
                })),
            };
        };

        session.model_id = Some(new_base.clone());
        session.effort = Some(new_effort.clone());
        let last_step_idx = session.last_step_idx;
        let conv_id = session.conversation_id.clone();

        self.persist_session(
            session_id,
            conv_id.as_deref(),
            last_step_idx,
            Some(&new_base),
            Some(&new_effort),
        );

        let config_options = self.session_config_options_json(Some(&new_base), Some(&new_effort));
        JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(json!({ "configOptions": config_options })),
            error: None,
        }
    }

    pub async fn handle_session_prompt(
        &mut self,
        id: Value,
        params: &Value,
        cancelled: Arc<AtomicBool>,
    ) -> Vec<String> {
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !session_id.is_empty() && !self.sessions.contains_key(session_id) {
            let _ = self.restore_session_state(session_id);
        }

        let prompt_text = params
            .get("prompt")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        let clean_prompt = prompt_text.trim();

        let mut args: Vec<String> = Vec::new();
        args.push("--add-dir".to_string());
        args.push(self.working_dir.clone());
        if let Ok(extra) = std::env::var("AGY_EXTRA_ARGS") {
            args.extend(extra.split_whitespace().map(String::from));
        }
        args.push("--output-format".to_string());
        args.push("stream-json".to_string());
        if let Some(session) = self.sessions.get(session_id) {
            if let Some(conv_id) = &session.conversation_id {
                args.push("--conversation".to_string());
                args.push(conv_id.clone());
            }
            if let Some(model_id) = &session.model_id {
                let stored_effort = session.effort.clone().unwrap_or_default();
                let (base, resolved_effort) = self.resolve_selection(Some(model_id));
                let efforts = self.efforts_for(&base);
                let effort = if efforts.contains(&stored_effort) {
                    stored_effort
                } else {
                    resolved_effort
                };
                args.push("--model".to_string());
                args.push(self.variant_for(&base, &effort));
            }
        }
        args.push("-p".to_string());
        args.push(clean_prompt.to_string());

        let spawn_result = Command::new("agy")
            .args(&args)
            .current_dir(&self.working_dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                return vec![serde_json::to_string(&JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(json!({"code":-32000,"message":format!("failed to run agy: {e}")})),
                })
                .unwrap()];
            }
        };

        let stdout = child.stdout.take();
        let skip_naration = self.skip_naration;
        let poll_session_id = session_id.to_string();
        let stdout_reader = tokio::spawn(async move {
            let mut processor = StreamProcessor::new(skip_naration);
            if let Some(stdout) = stdout {
                let mut lines = BufReader::new(stdout).lines();
                let mut out = io::stdout();
                while let Ok(Some(line)) = lines.next_line().await {
                    for notification in processor.process_line(&line, &poll_session_id) {
                        let _ = writeln!(out, "{}", notification);
                    }
                    let _ = out.flush();
                }
            }
            processor
        });

        let mut stderr = child.stderr.take();
        let stderr_reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(mut stderr) = stderr.take() {
                let _ = stderr.read_to_end(&mut buf).await;
            }
            buf
        });

        let mut was_cancelled = false;
        let result = tokio::select! {
            result = child.wait() => result,
            _ = async {
                while !cancelled.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            } => {
                was_cancelled = true;
                let _ = child.kill().await;
                child.wait().await
            }
        };
        let processor = stdout_reader
            .await
            .unwrap_or_else(|_| StreamProcessor::new(skip_naration));
        let stderr_bytes = stderr_reader.await.unwrap_or_default();

        let bound_conv_id = processor.conversation_id.clone();
        let new_step_idx = processor.last_step_idx;
        let had_updates = processor.had_updates;
        let result_failed = processor
            .result_status
            .as_deref()
            .is_some_and(|status| status == "ERROR");
        let result_error = processor.result_error.clone();

        if let Some(session) = self.sessions.get_mut(session_id) {
            if session.conversation_id.is_none() {
                session.conversation_id = bound_conv_id.clone();
            }
            if bound_conv_id.is_some() {
                session.last_step_idx = new_step_idx;
            }
        }
        if bound_conv_id.is_some() {
            let session = self.sessions.get(session_id);
            let model_id = session.and_then(|s| s.model_id.clone());
            let effort = session.and_then(|s| s.effort.clone());
            self.persist_session(
                session_id,
                bound_conv_id.as_deref(),
                new_step_idx,
                model_id.as_deref(),
                effort.as_deref(),
            );
        }

        let stop_reason = if was_cancelled {
            "cancelled"
        } else {
            "end_turn"
        };
        let output_lines = vec![serde_json::to_string(&JsonRpcResponse {
            jsonrpc: "2.0",
            id: id.clone(),
            result: Some(json!({ "stopReason": stop_reason })),
            error: None,
        })
        .unwrap()];

        match result {
            Ok(status) => {
                let stderr_text = String::from_utf8_lossy(&stderr_bytes);
                if !stderr_text.is_empty() {
                    eprintln!("[agy-acp] agy stderr: {}", stderr_text.trim_end());
                }

                if !was_cancelled && (!status.success() || result_failed) {
                    eprintln!("[agy-acp] WARN: agy exited with status: {}", status);
                    if !had_updates {
                        let msg = if let Some(error) = result_error.filter(|s| !s.is_empty()) {
                            format!("agy failed: {}", error.trim_end())
                        } else if stderr_text.is_empty() {
                            format!("agy exited with status: {}", status)
                        } else {
                            format!("agy failed: {}", stderr_text.trim_end())
                        };
                        return vec![serde_json::to_string(&JsonRpcResponse {
                            jsonrpc: "2.0",
                            id,
                            result: None,
                            error: Some(json!({"code":-32000,"message":msg})),
                        })
                        .unwrap()];
                    }
                }
            }
            Err(e) => {
                return vec![serde_json::to_string(&JsonRpcResponse {
                    jsonrpc: "2.0",
                    id,
                    result: None,
                    error: Some(
                        json!({"code":-32000,"message":format!("failed to wait for agy: {e}")}),
                    ),
                })
                .unwrap()];
            }
        }

        output_lines
    }
}

/// Parse `agy models` stdout into display names.
///
/// Each model line is `slug<TAB>Display Name`. ACP clients show `modelId` and
/// `name` side by side, so we keep only the display name and skip status lines
/// like "Fetching available models...".
pub fn parse_available_models(stdout: &str) -> Vec<String> {
    stdout.lines().filter_map(parse_model_line).collect()
}

fn parse_model_line(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    if let Some((_, name)) = line.split_once('\t') {
        let name = name.trim();
        if !name.is_empty() {
            return Some(name.to_string());
        }
    }
    if line.ends_with("...") {
        return None;
    }
    Some(line.to_string())
}

/// Filter out leading narration ("I will ...", "I'll ...") from response parts.
#[cfg(test)]
pub fn filter_narration(parts: &[String]) -> Option<String> {
    let text = parts
        .iter()
        .filter(|part| !is_narration(part))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

/// A part is considered narration if every non-empty line starts with "I will" or "I'll".
pub fn is_narration(text: &str) -> bool {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.is_empty() {
        return false;
    }
    lines.iter().all(|l| {
        let line = l.trim_start();
        line.starts_with("I will") || line.starts_with("I'll") || line.starts_with("I’ll")
    })
}
