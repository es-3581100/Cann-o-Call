use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(test)]
use uuid::Uuid;

const GENESIS_HASH: &str = "genesis";
const RECORD_VERSION: u8 = 1;
const CHECKPOINT_VERSION: u8 = 1;
#[cfg(test)]
const APPEND_ATOMICITY_CLASSIFICATION: &str = "PARTIAL_WRITE_POSSIBLE";

#[derive(Clone)]
struct AppState {
    ledger: PathBuf,
    checkpoints: PathBuf,
    accepted: LedgerState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LedgerState {
    seq: u64,
    head_hash: String,
    history_hash: String,
    accepted_count: u64,
}

impl LedgerState {
    fn genesis() -> Self {
        Self {
            seq: 0,
            head_hash: GENESIS_HASH.to_owned(),
            history_hash: GENESIS_HASH.to_owned(),
            accepted_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryClass {
    SequenceGap,
    DuplicateSequence,
    WrongPrevHash,
    BadContentHash,
    MalformedRecord,
    MalformedCheckpoint,
    StaleCheckpoint,
    DuplicateEvidenceID,
    TruncatedTail,
    Io,
}

#[derive(Debug)]
struct LedgerError {
    class: RecoveryClass,
    detail: String,
}

impl LedgerError {
    fn new(class: RecoveryClass, detail: impl Into<String>) -> Self {
        Self {
            class,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.class, self.detail)
    }
}

impl std::error::Error for LedgerError {}

impl From<io::Error> for LedgerError {
    fn from(err: io::Error) -> Self {
        Self::new(RecoveryClass::Io, err.to_string())
    }
}

// This is the complete deterministic input to the record hash. Evidence is
// supplied by Go after Go has made its admission decision; Rust does not add
// policy, timestamps, IDs, or other admission fields.
#[derive(Serialize)]
struct HashInput<'a> {
    version: u8,
    seq: u64,
    prev_hash: &'a str,
    evidence: &'a Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct LedgerRecord {
    version: u8,
    seq: u64,
    prev_hash: String,
    evidence: Value,
    hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u8,
    seq: u64,
    head_hash: String,
    history_hash: String,
}

#[derive(Debug)]
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

    fs::create_dir_all(ledger.parent().expect("ledger parent"))
        .expect("create sidecar event directory");
    fs::create_dir_all(&checkpoints).expect("create sidecar checkpoint directory");

    let accepted = open_verified_state(&ledger, &checkpoints)
        .unwrap_or_else(|err| panic!("strict ledger startup refused: {err}"));
    let state = Arc::new(Mutex::new(AppState {
        ledger,
        checkpoints,
        accepted,
    }));

    let app = Router::new()
        .route("/health", get(health))
        .route("/events", get(get_events).post(post_event))
        .route("/events/verify", get(verify_events))
        .route("/checkpoints", post(create_checkpoint))
        .with_state(state);

    let listen_addr =
        std::env::var("SIDECAR_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_owned());
    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("bind sidecar listener");
    println!("msl-ledger-sidecar listening on {listen_addr}");
    axum::serve(listener, app)
        .await
        .expect("run sidecar server");
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn get_events(
    State(state): State<Arc<Mutex<AppState>>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let state = state.lock().expect("state mutex poisoned");
    open_verified_state(&state.ledger, &state.checkpoints)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    let records = read_records(&state.ledger)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(Json(json!({ "events": records })))
}

async fn post_event(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(evidence): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let mut state = state.lock().expect("state mutex poisoned");
    let appended = append_evidence(&mut state, evidence)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "status": "recorded",
            "id": appended.id,
            "seq": appended.seq,
            "hash": appended.hash,
        })),
    ))
}

fn append_evidence(state: &mut AppState, evidence: Value) -> Result<AppendedEvent, LedgerError> {
    // Append atomicity is PARTIAL_WRITE_POSSIBLE: a prior uncertain/partial
    // write must not be papered over by another append.
    let observed = open_verified_state(&state.ledger, &state.checkpoints)?;
    if observed != state.accepted {
        return Err(LedgerError::new(
            RecoveryClass::StaleCheckpoint,
            "on-disk ledger does not match accepted in-memory state",
        ));
    }
    let records = read_records(&state.ledger)?;
    if let Some(id) = evidence_id(&evidence) {
        for existing in &records {
            if evidence_id(&existing.evidence) != Some(id) {
                continue;
            }
            if same_semantic_evidence(&existing.evidence, &evidence) {
                return Ok(AppendedEvent {
                    id: existing.evidence.get("id").cloned().unwrap_or(Value::Null),
                    seq: existing.seq,
                    hash: existing.hash.clone(),
                });
            }
            return Err(LedgerError::new(
                RecoveryClass::DuplicateEvidenceID,
                format!("evidence id {id:?} already binds different semantic evidence"),
            ));
        }
    }

    let candidate_seq = state.accepted.seq + 1;
    let candidate_prev_hash = state.accepted.head_hash.clone();
    let hash = hash_record(candidate_seq, &candidate_prev_hash, &evidence)?;
    let record = LedgerRecord {
        version: RECORD_VERSION,
        seq: candidate_seq,
        prev_hash: candidate_prev_hash,
        evidence: evidence.clone(),
        hash: hash.clone(),
    };
    let mut line = canonical_json(&record)?;
    line.push(b'\n');

    let existed = state.ledger.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&state.ledger)?;
    file.write_all(&line)?;
    file.sync_all()?;
    if !existed {
        sync_parent(&state.ledger)?;
    }

    // Commit accepted state only after open, write, and sync completed.
    state.accepted = advance(&state.accepted, &hash);
    Ok(AppendedEvent {
        id: evidence.get("id").cloned().unwrap_or(Value::Null),
        seq: candidate_seq,
        hash,
    })
}

fn evidence_id(evidence: &Value) -> Option<&str> {
    evidence
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

// Go may regenerate its transport envelope after it cannot observe a response
// from a committed Rust append. These fields are not the Go-approved semantic
// payload, so they must not turn that retry into a duplicate record. Every
// other field, notably details.accepted_transition, remains binding.
fn same_semantic_evidence(left: &Value, right: &Value) -> bool {
    fn normalized(evidence: &Value) -> Value {
        let mut normalized = evidence.clone();
        if let Some(object) = normalized.as_object_mut() {
            for field in ["seq", "created_at", "hash", "prev_hash", "rust_ack"] {
                object.remove(field);
            }
        }
        normalized
    }
    normalized(left) == normalized(right)
}

async fn verify_events(State(state): State<Arc<Mutex<AppState>>>) -> impl IntoResponse {
    let state = state.lock().expect("state mutex poisoned");
    match open_verified_state(&state.ledger, &state.checkpoints) {
        Ok(verified) => Json(json!({
            "ok": true,
            "events_checked": verified.accepted_count,
            "last_hash": verified.head_hash,
        })),
        Err(err) => Json(json!({
            "ok": false,
            "reason": err.to_string(),
            "recovery_class": format!("{:?}", err.class),
        })),
    }
}

async fn create_checkpoint(
    State(state): State<Arc<Mutex<AppState>>>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let state = state.lock().expect("state mutex poisoned");
    let observed = open_verified_state(&state.ledger, &state.checkpoints)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    if observed != state.accepted {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            LedgerError::new(
                RecoveryClass::StaleCheckpoint,
                "on-disk ledger does not match accepted in-memory state",
            )
            .to_string(),
        ));
    }
    let checkpoint = Checkpoint {
        version: CHECKPOINT_VERSION,
        seq: state.accepted.seq,
        head_hash: state.accepted.head_hash.clone(),
        history_hash: state.accepted.history_hash.clone(),
    };
    let path = state.checkpoints.join(checkpoint_file_name(&checkpoint));
    write_checkpoint(&path, &checkpoint)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "status": "checkpoint_created",
            "path": path,
            "checkpoint": checkpoint,
        })),
    ))
}

fn checkpoint_file_name(checkpoint: &Checkpoint) -> String {
    format!(
        "checkpoint-{}-{}.json",
        checkpoint.seq,
        short_hash(&checkpoint.head_hash)
    )
}

fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

fn write_checkpoint(path: &Path, checkpoint: &Checkpoint) -> Result<(), LedgerError> {
    let bytes = canonical_json(checkpoint)?;
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            sync_parent(path)?;
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let existing = read_checkpoint(path)?;
            if existing.seq == checkpoint.seq
                && existing.head_hash == checkpoint.head_hash
                && existing.history_hash == checkpoint.history_hash
                && existing.version == checkpoint.version
            {
                Ok(())
            } else {
                Err(LedgerError::new(
                    RecoveryClass::StaleCheckpoint,
                    format!(
                        "checkpoint path already binds different history: {}",
                        path.display()
                    ),
                ))
            }
        }
        Err(err) => Err(err.into()),
    }
}

fn sync_parent(path: &Path) -> Result<(), LedgerError> {
    File::open(path.parent().expect("path parent"))?.sync_all()?;
    Ok(())
}

fn open_verified_state(ledger: &Path, checkpoints: &Path) -> Result<LedgerState, LedgerError> {
    let records = read_records(ledger)?;
    let state = replay_records(&records)?;
    validate_checkpoints(checkpoints, &records)?;
    Ok(state)
}

#[cfg(test)]
fn replay_ledger(ledger: &Path) -> Result<LedgerState, LedgerError> {
    replay_records(&read_records(ledger)?)
}

fn read_records(ledger: &Path) -> Result<Vec<LedgerRecord>, LedgerError> {
    let bytes = match fs::read(ledger) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(LedgerError::new(
            RecoveryClass::TruncatedTail,
            "ledger does not end in a record delimiter",
        ));
    }
    let lines: Vec<_> = bytes.split(|byte| *byte == b'\n').collect();
    let mut records = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.is_empty() {
            if index == lines.len() - 1 {
                continue;
            }
            return Err(LedgerError::new(
                RecoveryClass::MalformedRecord,
                format!("line {}: empty record", index + 1),
            ));
        }
        let record = serde_json::from_slice::<LedgerRecord>(line).map_err(|err| {
            LedgerError::new(
                RecoveryClass::MalformedRecord,
                format!("line {}: {err}", index + 1),
            )
        })?;
        records.push(record);
    }
    Ok(records)
}

fn replay_records(records: &[LedgerRecord]) -> Result<LedgerState, LedgerError> {
    replay_from(LedgerState::genesis(), records)
}

fn replay_from(
    mut state: LedgerState,
    records: &[LedgerRecord],
) -> Result<LedgerState, LedgerError> {
    for (index, record) in records.iter().enumerate() {
        if record.version != RECORD_VERSION {
            return Err(LedgerError::new(
                RecoveryClass::MalformedRecord,
                format!(
                    "line {}: unsupported record version {}",
                    index + 1,
                    record.version
                ),
            ));
        }
        let expected_seq = state.seq + 1;
        if record.seq < expected_seq {
            return Err(LedgerError::new(
                RecoveryClass::DuplicateSequence,
                format!(
                    "line {}: expected seq {expected_seq}, got {}",
                    index + 1,
                    record.seq
                ),
            ));
        }
        if record.seq > expected_seq {
            return Err(LedgerError::new(
                RecoveryClass::SequenceGap,
                format!(
                    "line {}: expected seq {expected_seq}, got {}",
                    index + 1,
                    record.seq
                ),
            ));
        }
        if record.prev_hash != state.head_hash {
            return Err(LedgerError::new(
                RecoveryClass::WrongPrevHash,
                format!(
                    "line {}: expected previous hash {}",
                    index + 1,
                    state.head_hash
                ),
            ));
        }
        let computed = hash_record(record.seq, &record.prev_hash, &record.evidence)?;
        if record.hash != computed {
            return Err(LedgerError::new(
                RecoveryClass::BadContentHash,
                format!(
                    "line {}: stored record hash does not match deterministic input",
                    index + 1
                ),
            ));
        }
        state = advance(&state, &record.hash);
    }
    Ok(state)
}

fn advance(previous: &LedgerState, record_hash: &str) -> LedgerState {
    LedgerState {
        seq: previous.seq + 1,
        head_hash: record_hash.to_owned(),
        history_hash: hash_history(&previous.history_hash, record_hash),
        accepted_count: previous.accepted_count + 1,
    }
}

fn validate_checkpoints(checkpoints: &Path, records: &[LedgerRecord]) -> Result<(), LedgerError> {
    if !checkpoints.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(checkpoints)? {
        let path = entry?.path();
        if !path.is_file()
            || !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("checkpoint-"))
        {
            continue;
        }
        let checkpoint = read_checkpoint(&path)?;
        let prefix_len = usize::try_from(checkpoint.seq).map_err(|_| {
            LedgerError::new(
                RecoveryClass::StaleCheckpoint,
                "checkpoint sequence cannot fit platform",
            )
        })?;
        if prefix_len > records.len() {
            return Err(LedgerError::new(
                RecoveryClass::StaleCheckpoint,
                format!(
                    "{} claims unavailable seq {}",
                    path.display(),
                    checkpoint.seq
                ),
            ));
        }
        let prefix = replay_records(&records[..prefix_len])?;
        if checkpoint.version != CHECKPOINT_VERSION
            || checkpoint.seq != prefix.seq
            || checkpoint.head_hash != prefix.head_hash
            || checkpoint.history_hash != prefix.history_hash
        {
            return Err(LedgerError::new(
                RecoveryClass::StaleCheckpoint,
                format!("{} does not bind ledger history", path.display()),
            ));
        }
        // Prefix is verified before suffix: a checkpoint may safely be behind
        // the head, but it is never accepted merely because its file exists.
        replay_from(prefix, &records[prefix_len..]).map_err(|err| {
            LedgerError::new(
                err.class,
                format!("checkpoint suffix after {}: {}", path.display(), err.detail),
            )
        })?;
    }
    Ok(())
}

fn read_checkpoint(path: &Path) -> Result<Checkpoint, LedgerError> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err(LedgerError::new(
            RecoveryClass::MalformedCheckpoint,
            format!("{} is empty or unterminated", path.display()),
        ));
    }
    let trimmed = &bytes[..bytes.len() - 1];
    serde_json::from_slice(trimmed).map_err(|err| {
        LedgerError::new(
            RecoveryClass::MalformedCheckpoint,
            format!("{}: {err}", path.display()),
        )
    })
}

fn hash_record(seq: u64, prev_hash: &str, evidence: &Value) -> Result<String, LedgerError> {
    Ok(hex_sha256(&canonical_json(&HashInput {
        version: RECORD_VERSION,
        seq,
        prev_hash,
        evidence,
    })?))
}

fn hash_history(previous_history_hash: &str, record_hash: &str) -> String {
    hex_sha256(format!("{previous_history_hash}\n{record_hash}").as_bytes())
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, LedgerError> {
    serde_json::to_vec(value)
        .map_err(|err| LedgerError::new(RecoveryClass::BadContentHash, err.to_string()))
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDataDir(PathBuf);

    impl TestDataDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("msl-ledger-sidecar-{}", Uuid::new_v4()));
            fs::create_dir_all(path.join("events")).unwrap();
            fs::create_dir_all(path.join("checkpoints")).unwrap();
            Self(path)
        }

        fn ledger(&self) -> PathBuf {
            self.0.join("events/ledger.jsonl")
        }

        fn checkpoints(&self) -> PathBuf {
            self.0.join("checkpoints")
        }

        fn state(&self) -> AppState {
            AppState {
                ledger: self.ledger(),
                checkpoints: self.checkpoints(),
                accepted: open_verified_state(&self.ledger(), &self.checkpoints()).unwrap(),
            }
        }
    }

    impl Drop for TestDataDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn append(state: &mut AppState, kind: &str) -> AppendedEvent {
        append_evidence(state, json!({"id": kind, "type": kind})).unwrap()
    }

    fn write_records(path: &Path, records: &[LedgerRecord]) {
        let mut bytes = Vec::new();
        for record in records {
            bytes.extend(canonical_json(record).unwrap());
            bytes.push(b'\n');
        }
        fs::write(path, bytes).unwrap();
    }

    fn assert_class(result: Result<LedgerState, LedgerError>, class: RecoveryClass) {
        assert_eq!(result.unwrap_err().class, class);
    }

    #[test]
    fn genesis_and_short_id_checkpoint_are_valid() {
        let data = TestDataDir::new();
        let state = data.state();
        assert_eq!(state.accepted, LedgerState::genesis());
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            seq: 0,
            head_hash: GENESIS_HASH.into(),
            history_hash: GENESIS_HASH.into(),
        };
        let path = data.checkpoints().join(checkpoint_file_name(&checkpoint));
        write_checkpoint(&path, &checkpoint).unwrap();
        assert_eq!(short_hash("x"), "x");
        assert_eq!(
            open_verified_state(&data.ledger(), &data.checkpoints()).unwrap(),
            state.accepted
        );
    }

    #[test]
    fn one_many_and_restart_replay_exact_accepted_state() {
        let data = TestDataDir::new();
        let mut state = data.state();
        let first = append(&mut state, "one");
        assert_eq!(first.seq, 1);
        append(&mut state, "two");
        append(&mut state, "three");
        let restarted = data.state();
        assert_eq!(restarted.accepted, state.accepted);
        assert_eq!(restarted.accepted.accepted_count, 3);
    }

    #[test]
    fn duplicate_evidence_id_returns_original_ack_and_rejects_semantic_conflict() {
        let data = TestDataDir::new();
        let mut state = data.state();
        let first = append_evidence(
            &mut state,
            json!({
                "id": "transition-1", "seq": 1, "created_at": "2026-08-31T00:00:00Z",
                "type": "transition.authority.accepted", "hash": "first-go-envelope",
                "details": {"accepted_transition": {"proposal": {"transition_id": "transition-1"}}}
            }),
        )
        .unwrap();
        let retry = append_evidence(
            &mut state,
            json!({
                "id": "transition-1", "seq": 1, "created_at": "2026-08-31T00:01:00Z",
                "type": "transition.authority.accepted", "hash": "retried-go-envelope",
                "details": {"accepted_transition": {"proposal": {"transition_id": "transition-1"}}}
            }),
        )
        .unwrap();
        assert_eq!(retry.seq, first.seq);
        assert_eq!(retry.hash, first.hash);
        assert_eq!(state.accepted.accepted_count, 1);
        assert_eq!(read_records(&data.ledger()).unwrap().len(), 1);

        let conflict = append_evidence(
            &mut state,
            json!({
                "id": "transition-1", "type": "transition.authority.accepted",
                "details": {"accepted_transition": {"proposal": {"transition_id": "different"}}}
            }),
        )
        .unwrap_err();
        assert_eq!(conflict.class, RecoveryClass::DuplicateEvidenceID);
        assert_eq!(state.accepted.accepted_count, 1);
    }

    #[test]
    fn verified_checkpoint_and_suffix_replay() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "one");
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            seq: state.accepted.seq,
            head_hash: state.accepted.head_hash.clone(),
            history_hash: state.accepted.history_hash.clone(),
        };
        write_checkpoint(
            &data.checkpoints().join(checkpoint_file_name(&checkpoint)),
            &checkpoint,
        )
        .unwrap();
        append(&mut state, "two");
        let restarted = data.state();
        assert_eq!(restarted.accepted, state.accepted);
        assert_eq!(restarted.accepted.seq, 2);
    }

    #[test]
    fn failed_open_does_not_advance_and_retry_uses_exactly_next_sequence() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "first");
        let saved = data.ledger().with_extension("saved");
        fs::rename(data.ledger(), &saved).unwrap();
        fs::create_dir(data.ledger()).unwrap();
        let before = state.accepted.clone();
        assert_eq!(
            append_evidence(&mut state, json!({"id": "failed"}))
                .unwrap_err()
                .class,
            RecoveryClass::Io
        );
        assert_eq!(state.accepted, before);
        fs::remove_dir(data.ledger()).unwrap();
        fs::rename(saved, data.ledger()).unwrap();
        assert_eq!(append(&mut state, "retry").seq, 2);
        assert_eq!(state.accepted.accepted_count, 2);
    }

    #[test]
    fn failed_append_can_checkpoint_prior_accepted_head() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "first");
        let saved = data.ledger().with_extension("saved");
        fs::rename(data.ledger(), &saved).unwrap();
        fs::create_dir(data.ledger()).unwrap();
        assert!(append_evidence(&mut state, json!({"id": "failed"})).is_err());
        fs::remove_dir(data.ledger()).unwrap();
        fs::rename(saved, data.ledger()).unwrap();
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            seq: state.accepted.seq,
            head_hash: state.accepted.head_hash.clone(),
            history_hash: state.accepted.history_hash.clone(),
        };
        write_checkpoint(
            &data.checkpoints().join(checkpoint_file_name(&checkpoint)),
            &checkpoint,
        )
        .unwrap();
        assert_eq!(data.state().accepted, state.accepted);
    }

    #[tokio::test]
    async fn checkpoint_handler_persists_verified_current_state() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "first");
        let app_state = Arc::new(Mutex::new(state));

        let response = create_checkpoint(State(app_state)).await.unwrap();
        assert_eq!(response.0, StatusCode::CREATED);
        assert_eq!(fs::read_dir(data.checkpoints()).unwrap().count(), 1);
        assert_eq!(data.state().accepted.seq, 1);
    }

    #[tokio::test]
    async fn checkpoint_handler_rejects_tampered_disk_history_without_emitting_checkpoint() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "first");
        let app_state = Arc::new(Mutex::new(state));
        let mut records = read_records(&data.ledger()).unwrap();
        records[0].evidence = json!({"id": "tampered"});
        write_records(&data.ledger(), &records);

        let error = create_checkpoint(State(app_state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(error.1.contains("BadContentHash"));
        assert_eq!(fs::read_dir(data.checkpoints()).unwrap().count(), 0);
    }

    #[test]
    fn partial_write_is_classified_without_claiming_append_atomicity() {
        let data = TestDataDir::new();
        // Isolated crash fixture: normal write+sync cannot prove OS append atomicity.
        assert_eq!(APPEND_ATOMICITY_CLASSIFICATION, "PARTIAL_WRITE_POSSIBLE");
        fs::write(data.ledger(), b"{\"version\":1").unwrap();
        assert_class(replay_ledger(&data.ledger()), RecoveryClass::TruncatedTail);
    }

    #[test]
    fn duplicate_sequence_and_gap_have_distinct_classifications() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "one");
        let mut records = read_records(&data.ledger()).unwrap();
        records.push(records[0].clone());
        write_records(&data.ledger(), &records);
        assert_class(
            replay_ledger(&data.ledger()),
            RecoveryClass::DuplicateSequence,
        );
        records.pop();
        records[0].seq = 2;
        records[0].hash =
            hash_record(records[0].seq, &records[0].prev_hash, &records[0].evidence).unwrap();
        write_records(&data.ledger(), &records);
        assert_class(replay_ledger(&data.ledger()), RecoveryClass::SequenceGap);
    }

    #[test]
    fn wrong_previous_hash_and_bad_content_hash_fail_strict_replay() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "one");
        let mut records = read_records(&data.ledger()).unwrap();
        records[0].prev_hash = "wrong".into();
        write_records(&data.ledger(), &records);
        assert_class(replay_ledger(&data.ledger()), RecoveryClass::WrongPrevHash);
        records[0].prev_hash = GENESIS_HASH.into();
        records[0].evidence = json!({"id": "tampered"});
        write_records(&data.ledger(), &records);
        assert_class(replay_ledger(&data.ledger()), RecoveryClass::BadContentHash);
    }

    #[test]
    fn malformed_record_and_checkpoint_are_startup_failures() {
        let data = TestDataDir::new();
        fs::write(data.ledger(), b"not-json\n").unwrap();
        assert_class(
            open_verified_state(&data.ledger(), &data.checkpoints()),
            RecoveryClass::MalformedRecord,
        );
        fs::write(data.ledger(), b"\n").unwrap();
        assert_class(
            open_verified_state(&data.ledger(), &data.checkpoints()),
            RecoveryClass::MalformedRecord,
        );
        fs::write(data.ledger(), b"").unwrap();
        fs::write(
            data.checkpoints().join("checkpoint-bad.json"),
            b"not-json\n",
        )
        .unwrap();
        assert_class(
            open_verified_state(&data.ledger(), &data.checkpoints()),
            RecoveryClass::MalformedCheckpoint,
        );
    }

    #[test]
    fn stale_checkpoint_cannot_be_adopted() {
        let data = TestDataDir::new();
        let mut state = data.state();
        append(&mut state, "one");
        let stale = Checkpoint {
            version: CHECKPOINT_VERSION,
            seq: 1,
            head_hash: "not-the-ledger-head".into(),
            history_hash: state.accepted.history_hash.clone(),
        };
        write_checkpoint(
            &data.checkpoints().join(checkpoint_file_name(&stale)),
            &stale,
        )
        .unwrap();
        assert_class(
            open_verified_state(&data.ledger(), &data.checkpoints()),
            RecoveryClass::StaleCheckpoint,
        );
    }

    #[test]
    fn bounded_fixed_seed_model_preserves_head_and_count_invariants() {
        let data = TestDataDir::new();
        let mut state = data.state();
        let mut seed = 0x5eed_u64;
        for ordinal in 0..64 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let evidence = json!({"id": format!("op-{ordinal}"), "value": seed % 17});
            let appended = append_evidence(&mut state, evidence).unwrap();
            assert_eq!(appended.seq, ordinal + 1);
            assert_eq!(state.accepted.seq, state.accepted.accepted_count);
        }
        assert_eq!(replay_ledger(&data.ledger()).unwrap(), state.accepted);
    }

    #[tokio::test]
    async fn go_forwarding_contract_preserves_approved_evidence_and_rejects_write_without_phantom()
    {
        let data = TestDataDir::new();
        let app_state = Arc::new(Mutex::new(data.state()));
        // Exact Go Event JSON shape: Rust treats this as opaque evidence rather
        // than re-admitting it or changing Go's policy decision.
        let approved = json!({
            "id": "go-approved-1", "seq": 1, "created_at": "2026-08-31T00:00:00Z",
            "type": "transition", "action": "apply", "status": "accepted",
            "prev_hash": "genesis", "hash": "go-local-hash"
        });
        let response = post_event(State(app_state.clone()), Json(approved.clone()))
            .await
            .unwrap();
        assert_eq!(response.0, StatusCode::CREATED);
        assert_eq!(response.1 .0["seq"], 1);
        let records = read_records(&data.ledger()).unwrap();
        assert_eq!(records[0].evidence, approved);
        assert_eq!(replay_ledger(&data.ledger()).unwrap().seq, 1);

        let saved = data.ledger().with_extension("saved");
        fs::rename(data.ledger(), &saved).unwrap();
        fs::create_dir(data.ledger()).unwrap();
        let rejected = post_event(State(app_state.clone()), Json(json!({"id": "go-retry"}))).await;
        assert_eq!(rejected.unwrap_err().0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(app_state.lock().unwrap().accepted.seq, 1);
        fs::remove_dir(data.ledger()).unwrap();
        fs::rename(saved, data.ledger()).unwrap();
        let retry = post_event(State(app_state.clone()), Json(json!({"id": "go-retry"})))
            .await
            .unwrap();
        assert_eq!(retry.1 .0["seq"], 2);
        assert_eq!(data.state().accepted, app_state.lock().unwrap().accepted);
    }
}
