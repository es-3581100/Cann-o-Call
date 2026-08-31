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
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    ledger: PathBuf,
    checkpoints: PathBuf,
    last_hash: String,
    seq: u64,
}

#[tokio::main]
async fn main() {
    let data_dir = std::env::var("SIDECAR_DATA_DIR").unwrap_or_else(|_| "data-sidecar".into());

    let ledger = PathBuf::from(&data_dir)
        .join("events")
        .join("ledger.jsonl");

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
) -> impl IntoResponse {
    let mut state = state.lock().unwrap();

    state.seq += 1;

    if event.get("id").is_none() {
        event["id"] = json!(Uuid::new_v4().to_string());
    }

    if let Some(obj) = event.as_object_mut() {
        obj.remove("hash");
    }

    event["seq"] = json!(state.seq);
    event["prev_hash"] = json!(state.last_hash);
    event["recorded_at"] = json!(chrono::Utc::now().to_rfc3339());
    event["authority"] = json!("rust_sidecar");

    let hash_payload = serde_json::to_string(&event).unwrap_or_default();
    let hash = hex_sha256(hash_payload.as_bytes());

    event["hash"] = json!(hash);

    let line = serde_json::to_string(&event).unwrap_or_default();

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&state.ledger)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    writeln!(file, "{}", line)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    state.last_hash = hash.clone();

    (
        StatusCode::CREATED,
        Json(json!({
            "status": "recorded",
            "id": event["id"],
            "seq": state.seq,
            "hash": hash
        })),
    )
}


async fn verify_events(
    State(state): State<Arc<Mutex<AppState>>>,
) -> impl IntoResponse {
    let state = state.lock().unwrap();

    let mut prev = "genesis".to_string();
    let mut expected_seq = 1u64;
    let mut checked = 0u64;

    let file = match File::open(&state.ledger) {
        Ok(f) => f,
        Err(_) => {
            return Json(json!({
                "ok": true,
                "events_checked": 0,
                "last_hash": prev
            }))
        }
    };

    let reader = BufReader::new(file);

    for line in reader.lines().flatten() {
        let mut value: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(err) => {
                return Json(json!({
                    "ok": false,
                    "reason": format!("invalid json: {}", err),
                    "events_checked": checked
                }))
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

        let stored_seq = value
            .get("seq")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if stored_prev != prev {
            return Json(json!({
                "ok": false,
                "reason": "prev_hash mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            }));
        }

        if stored_seq != expected_seq {
            return Json(json!({
                "ok": false,
                "reason": "seq mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            }));
        }

        if let Some(obj) = value.as_object_mut() {
            obj.remove("hash");
        }

        let payload = serde_json::to_string(&value).unwrap_or_default();
        let computed = hex_sha256(payload.as_bytes());

        if computed != stored_hash {
            return Json(json!({
                "ok": false,
                "reason": "hash mismatch",
                "failed_seq": stored_seq,
                "events_checked": checked
            }));
        }

        prev = stored_hash;
        expected_seq += 1;
        checked += 1;
    }

    Json(json!({
        "ok": true,
        "events_checked": checked,
        "last_hash": prev
    }))
}

async fn create_checkpoint(
    State(state): State<Arc<Mutex<AppState>>>,
) -> impl IntoResponse {
    let state = state.lock().unwrap();

    let checkpoint = json!({
        "created_at": chrono::Utc::now().to_rfc3339(),
        "seq": state.seq,
        "last_hash": state.last_hash,
    });

    let name = format!(
        "checkpoint-{}-{}.json",
        state.seq,
        &state.last_hash[..12]
    );

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

    result
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}
