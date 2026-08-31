use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    ledger: PathBuf,
    checkpoints: PathBuf,
    last_hash: String,
    seq: u64,
}

struct AppendedEvent {
    id: Value,
    seq: u64,
    hash: String,
}

#[tokio::main]
async fn main() {
    let data_dir = std::env::var("SIDECAR_DATA_DIR").unwrap_or_else(|_| "data-sidecar".into());

    let ledger = PathBuf::from(&data_dir).join("events").join("ledger.jsonl");

    let checkpoints = PathBuf::from(&data_dir).join("checkpoints");

    fs::create_dir_all(ledger.parent().unwrap()).expect("create sidecar event directory");
    fs::create_dir_all(&checkpoints).expect("create sidecar checkpoint directory");

    let mut state = AppState {
        ledger,
        checkpoints,
        last_hash: "genesis".into(),
        seq: 0,
    };

    load_existing_ledger(&mut state);

    let state = Arc::new(Mutex::new(state));

    let app = Router::new()
        .route("/health", get(health))
        .route("/events", get(get_events).post(post_event))
        .route("/events/verify", get(verify_events))
        .route("/checkpoints", post(create_checkpoint))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .expect("bind sidecar listener");

    println!("msl-ledger-sidecar listening on 0.0.0.0:9090");

    axum::serve(listener, app)
        .await
        .expect("run sidecar server");
}

fn load_existing_ledger(state: &mut AppState) {
    let Ok(file) = File::open(&state.ledger) else {
        return;
    };

    let reader = BufReader::new(file);

    for line in reader.lines().flatten() {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };

        if let Some(seq) = value.get("seq").and_then(|v| v.as_u64()) {
            if seq > state.seq {
                state.seq = seq;
            }
        }

        if let Some(hash) = value.get("hash").and_then(|v| v.as_str()) {
            state.last_hash = hash.to_string();
        }
    }
}

async fn health() -> impl IntoResponse {
    Json(json!({
        "status": "ok"
    }))
}

async fn get_events(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().unwrap();

    let mut events = Vec::new();

    if let Ok(file) = File::open(&state.ledger) {
        let reader = BufReader::new(file);

        for line in reader.lines().flatten() {
            if let Ok(value) = serde_json::from_str::<Value>(&line) {
                events.push(value);
            }
        }
    }

    Json(json!({
        "events": events
    }))
}

async fn post_event(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(mut event): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let mut state = state.lock().unwrap();

    let appended = append_event(&mut state, &mut event)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "status": "recorded",
            "id": appended.id,
            "seq": appended.seq,
            "hash": appended.hash
        })),
    ))
}

fn append_event(state: &mut AppState, event: &mut Value) -> io::Result<AppendedEvent> {
    let candidate_seq = state.seq + 1;
    let candidate_prev_hash = state.last_hash.clone();

    if event.get("id").is_none() {
        event["id"] = json!(Uuid::new_v4().to_string());
    }

    if let Some(obj) = event.as_object_mut() {
        obj.remove("hash");
    }

    event["seq"] = json!(candidate_seq);
    event["prev_hash"] = json!(candidate_prev_hash);
    event["recorded_at"] = json!(chrono::Utc::now().to_rfc3339());
    event["authority"] = json!("rust_sidecar");

    let hash_payload = serde_json::to_string(&event).unwrap_or_default();
    let hash = hex_sha256(hash_payload.as_bytes());

    event["hash"] = json!(hash);

    let line = serde_json::to_string(&event).unwrap_or_default();

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&state.ledger)?;

    writeln!(file, "{}", line)?;
    file.sync_data()?;

    state.seq = candidate_seq;
    state.last_hash = hash.clone();

    Ok(AppendedEvent {
        id: event["id"].clone(),
        seq: candidate_seq,
        hash,
    })
}

async fn verify_events(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().unwrap();

    Json(verify_ledger(&state.ledger))
}

fn verify_ledger(ledger: &Path) -> Value {
    let mut prev = "genesis".to_string();
    let mut expected_seq = 1u64;
    let mut checked = 0u64;

    let file = match File::open(ledger) {
        Ok(f) => f,
        Err(_) => {
            return json!({
                "ok": true,
                "events_checked": 0,
                "last_hash": prev
            })
        }
    };

    let reader = BufReader::new(file);

    for line in reader.lines().flatten() {
        let mut value: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(err) => {
                return json!({
                    "ok": false,
                    "reason": format!("invalid json: {}", err),
                    "events_checked": checked
                })
            }
        };

        let stored_hash = value
            .get("hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let stored_prev = value
            .get("prev_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let stored_seq = value.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);

        if stored_prev != prev {
            return json!({
                "ok": false,
                "reason": "prev_hash mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            });
        }

        if stored_seq != expected_seq {
            return json!({
                "ok": false,
                "reason": "seq mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            });
        }

        if let Some(obj) = value.as_object_mut() {
            obj.remove("hash");
        }

        let payload = serde_json::to_string(&value).unwrap_or_default();
        let computed = hex_sha256(payload.as_bytes());

        if computed != stored_hash {
            return json!({
                "ok": false,
                "reason": "hash mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            });
        }

        prev = stored_hash;
        expected_seq += 1;
        checked += 1;
    }

    json!({
        "ok": true,
        "events_checked": checked,
        "last_hash": prev
    })
}

async fn create_checkpoint(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().unwrap();

    let checkpoint = json!({
        "created_at": chrono::Utc::now().to_rfc3339(),
        "seq": state.seq,
        "last_hash": state.last_hash,
    });

    let name = format!("checkpoint-{}-{}.json", state.seq, &state.last_hash[..12]);

    let path = state.checkpoints.join(name);

    if let Err(err) = fs::write(&path, serde_json::to_string_pretty(&checkpoint).unwrap()) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": err.to_string()
            })),
        );
    }

    (
        StatusCode::CREATED,
        Json(json!({
            "status": "checkpoint_created",
            "path": path,
            "checkpoint": checkpoint
        })),
    )
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();

    result.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    struct TestDataDir(PathBuf);

    impl TestDataDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("msl-ledger-sidecar-{}", Uuid::new_v4()));
            fs::create_dir_all(path.join("events")).unwrap();
            fs::create_dir_all(path.join("checkpoints")).unwrap();
            Self(path)
        }

        fn state(&self) -> AppState {
            AppState {
                ledger: self.0.join("events/ledger.jsonl"),
                checkpoints: self.0.join("checkpoints"),
                last_hash: "genesis".into(),
                seq: 0,
            }
        }
    }

    impl Drop for TestDataDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn event_count(ledger: &Path) -> usize {
        fs::read_to_string(ledger)
            .map(|contents| contents.lines().count())
            .unwrap_or(0)
    }

    fn force_append_failure(ledger: &Path) -> PathBuf {
        let saved = ledger.with_extension("jsonl.saved");
        fs::rename(ledger, &saved).unwrap();
        fs::create_dir(ledger).unwrap();
        saved
    }

    fn restore_ledger(ledger: &Path, saved: &Path) {
        fs::remove_dir(ledger).unwrap();
        fs::rename(saved, ledger).unwrap();
    }

    #[tokio::test]
    async fn failed_append_preserves_accepted_seq_head_and_count() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        let _ = post_event(State(app_state.clone()), Json(json!({"type": "first"})))
            .await
            .unwrap();

        let (ledger, before_seq, before_head) = {
            let state = app_state.lock().unwrap();
            (state.ledger.clone(), state.seq, state.last_hash.clone())
        };
        let saved = force_append_failure(&ledger);

        let error = post_event(State(app_state.clone()), Json(json!({"type": "failed"})))
            .await
            .unwrap_err();

        assert_eq!(error.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(error.1.contains("Is a directory"));
        let state = app_state.lock().unwrap();
        assert_eq!(state.seq, before_seq);
        assert_eq!(state.last_hash, before_head);
        assert_eq!(event_count(&saved), 1);
    }

    #[tokio::test]
    async fn retry_uses_next_accepted_sequence_and_chain_verifies() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        let _ = post_event(State(app_state.clone()), Json(json!({"type": "first"})))
            .await
            .unwrap();
        let ledger = app_state.lock().unwrap().ledger.clone();
        let saved = force_append_failure(&ledger);

        post_event(State(app_state.clone()), Json(json!({"type": "failed"})))
            .await
            .unwrap_err();
        restore_ledger(&ledger, &saved);

        let retry = post_event(State(app_state.clone()), Json(json!({"type": "retry"})))
            .await
            .unwrap();

        assert_eq!(retry.1 .0["seq"], 2);
        assert_eq!(event_count(&ledger), 2);
        assert_eq!(verify_ledger(&ledger)["ok"], true);
    }

    #[tokio::test]
    async fn failed_retry_restart_recovers_accepted_state_and_verifies() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        let _ = post_event(State(app_state.clone()), Json(json!({"type": "first"})))
            .await
            .unwrap();
        let ledger = app_state.lock().unwrap().ledger.clone();
        let saved = force_append_failure(&ledger);

        post_event(State(app_state.clone()), Json(json!({"type": "failed"})))
            .await
            .unwrap_err();
        restore_ledger(&ledger, &saved);
        let _ = post_event(State(app_state), Json(json!({"type": "retry"})))
            .await
            .unwrap();

        let mut restarted = data_dir.state();
        load_existing_ledger(&mut restarted);

        assert_eq!(restarted.seq, 2);
        assert_ne!(restarted.last_hash, "genesis");
        assert_eq!(verify_ledger(&ledger)["ok"], true);
        assert_eq!(verify_ledger(&ledger)["events_checked"], 2);
    }

    #[tokio::test]
    async fn two_failures_then_success_consumes_exactly_one_ordinal() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        let _ = post_event(State(app_state.clone()), Json(json!({"type": "first"})))
            .await
            .unwrap();
        let ledger = app_state.lock().unwrap().ledger.clone();
        let saved = force_append_failure(&ledger);

        for event_type in ["failed-one", "failed-two"] {
            post_event(State(app_state.clone()), Json(json!({"type": event_type})))
                .await
                .unwrap_err();
        }
        restore_ledger(&ledger, &saved);
        let _ = post_event(State(app_state.clone()), Json(json!({"type": "success"})))
            .await
            .unwrap();

        let events: Vec<Value> = fs::read_to_string(&ledger)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["seq"], 1);
        assert_eq!(events[1]["seq"], 2);
        assert_eq!(app_state.lock().unwrap().seq, 2);
    }

    #[test]
    fn concurrent_appends_are_contiguous() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        let handles: Vec<_> = (0..3)
            .map(|ordinal| {
                let app_state = app_state.clone();
                std::thread::spawn(move || {
                    let mut event = json!({"type": format!("concurrent-{ordinal}")});
                    let mut state = app_state.lock().unwrap();
                    let _ = append_event(&mut state, &mut event).unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let ledger = app_state.lock().unwrap().ledger.clone();
        assert_eq!(event_count(&ledger), 3);
        assert_eq!(verify_ledger(&ledger)["ok"], true);
        assert_eq!(verify_ledger(&ledger)["events_checked"], 3);
    }

    #[tokio::test]
    async fn append_accepts_empty_null_and_large_values_contiguously() {
        let data_dir = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data_dir.state()));

        for event in [json!(null), json!({}), json!({"payload": "x".repeat(8192)})] {
            let _ = post_event(State(app_state.clone()), Json(event))
                .await
                .unwrap();
        }

        let ledger = app_state.lock().unwrap().ledger.clone();
        assert_eq!(event_count(&ledger), 3);
        assert_eq!(verify_ledger(&ledger)["ok"], true);
        assert_eq!(verify_ledger(&ledger)["events_checked"], 3);
    }
}
