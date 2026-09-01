package eventlog

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

type sidecarProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	mu      sync.Mutex
	waitErr error
	output  bytes.Buffer
}

type rustVerifyResponse struct {
	OK            bool   `json:"ok"`
	EventsChecked uint64 `json:"events_checked"`
	LastHash      string `json:"last_hash"`
}

type rustEventsResponse struct {
	Events []struct {
		Seq      uint64          `json:"seq"`
		PrevHash string          `json:"prev_hash"`
		Hash     string          `json:"hash"`
		Evidence json.RawMessage `json:"evidence"`
	} `json:"events"`
}

func TestGoApprovedEventUsesLiveRustSidecarAndSurvivesRestart(t *testing.T) {
	repoRoot := sidecarRepoRoot(t)
	listenAddr := reserveLoopbackAddress(t)
	rustDataDir := t.TempDir()
	goDataDir := t.TempDir()

	sidecar := startSidecar(t, repoRoot, rustDataDir, listenAddr)
	t.Cleanup(func() { sidecar.stop(t) })
	sidecarURL := "http://" + listenAddr
	waitForSidecarHealth(t, sidecar, sidecarURL)

	service, err := New(goDataDir, sidecarURL)
	if err != nil {
		t.Fatalf("new Go event log: %v", err)
	}
	if _, err := service.Append(Event{}); err == nil {
		t.Fatal("invalid Go event must be rejected before live Rust forwarding")
	}
	if verify := getRustVerify(t, sidecarURL); !verify.OK || verify.EventsChecked != 0 {
		t.Fatalf("invalid Go event reached Rust: %#v", verify)
	}

	firstInput := Event{
		ID:        "go-approved-1",
		Type:      "transition",
		Action:    "apply",
		Status:    "accepted",
		Details:   map[string]any{"ticket": "CHUNK-02"},
		CreatedAt: time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC),
	}
	first, err := service.Append(firstInput)
	if err != nil {
		t.Fatalf("append approved Go event to live Rust sidecar: %v", err)
	}
	if first.Seq != 1 {
		t.Fatalf("first Go sequence = %d, want 1", first.Seq)
	}
	firstRust := getRustEvents(t, sidecarURL)
	if len(firstRust.Events) != 1 || firstRust.Events[0].Seq != 1 || firstRust.Events[0].PrevHash != "genesis" {
		t.Fatalf("first Rust record is not contiguous: %#v", firstRust.Events)
	}
	var forwardedFirst Event
	if err := json.Unmarshal(firstRust.Events[0].Evidence, &forwardedFirst); err != nil {
		t.Fatalf("decode Rust evidence carrying Go event: %v", err)
	}
	if forwardedFirst.ID != first.ID || forwardedFirst.Action != first.Action || forwardedFirst.Status != first.Status || forwardedFirst.Details["ticket"] != "CHUNK-02" || forwardedFirst.Seq != 1 {
		t.Fatalf("Rust evidence did not preserve Go-approved details: %#v", forwardedFirst)
	}

	ledger := filepath.Join(rustDataDir, "events", "ledger.jsonl")
	savedLedger := ledger + ".saved"
	if err := os.Rename(ledger, savedLedger); err != nil {
		t.Fatalf("isolate Rust write failure: %v", err)
	}
	if err := os.Mkdir(ledger, 0o755); err != nil {
		t.Fatalf("replace Rust ledger with failure fixture: %v", err)
	}
	secondInput := Event{
		ID:        "go-approved-2",
		Type:      "transition",
		Action:    "apply",
		Status:    "accepted",
		CreatedAt: time.Date(2026, time.August, 31, 12, 1, 0, 0, time.UTC),
	}
	if _, err := service.Append(secondInput); err == nil {
		t.Fatal("actual Rust write failure must propagate to Go")
	}
	if events, err := service.List(); err != nil || len(events) != 1 {
		t.Fatalf("Rust write failure created Go phantom: events=%d err=%v", len(events), err)
	}
	if err := os.Remove(ledger); err != nil {
		t.Fatalf("remove Rust write failure fixture: %v", err)
	}
	if err := os.Rename(savedLedger, ledger); err != nil {
		t.Fatalf("restore Rust ledger after write failure: %v", err)
	}

	second, err := service.Append(secondInput)
	if err != nil {
		t.Fatalf("retry approved Go event to live Rust sidecar: %v", err)
	}
	if second.Seq != 2 {
		t.Fatalf("retry Go sequence = %d, want 2", second.Seq)
	}
	if events, err := service.List(); err != nil || len(events) != 2 {
		t.Fatalf("Go accepted state after retry: events=%d err=%v", len(events), err)
	}
	beforeRestart := getRustVerify(t, sidecarURL)
	if !beforeRestart.OK || beforeRestart.EventsChecked != 2 {
		t.Fatalf("Rust accepted state before restart: %#v", beforeRestart)
	}
	beforeRecords := getRustEvents(t, sidecarURL)
	if len(beforeRecords.Events) != 2 || beforeRecords.Events[1].Seq != 2 || beforeRecords.Events[1].PrevHash != beforeRecords.Events[0].Hash || beforeRestart.LastHash != beforeRecords.Events[1].Hash {
		t.Fatalf("Rust retry did not produce exactly contiguous head: %#v", beforeRecords.Events)
	}

	sidecar.stop(t)
	sidecar = startSidecar(t, repoRoot, rustDataDir, listenAddr)
	t.Cleanup(func() { sidecar.stop(t) })
	waitForSidecarHealth(t, sidecar, sidecarURL)
	afterRestart := getRustVerify(t, sidecarURL)
	if !afterRestart.OK || afterRestart.EventsChecked != beforeRestart.EventsChecked || afterRestart.LastHash != beforeRestart.LastHash {
		t.Fatalf("Rust state changed after restart: before=%#v after=%#v", beforeRestart, afterRestart)
	}
}

func sidecarRepoRoot(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate sidecar integration test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "sidecar", "Cargo.toml")); err != nil {
		t.Fatalf("locate sidecar Cargo manifest: %v", err)
	}
	return root
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved loopback address: %v", err)
	}
	return address
}

func startSidecar(t *testing.T, repoRoot, dataDir, listenAddr string) *sidecarProcess {
	t.Helper()
	process := &sidecarProcess{done: make(chan struct{})}
	process.cmd = exec.Command(
		"cargo",
		"run",
		"--locked",
		"--manifest-path",
		filepath.Join(repoRoot, "sidecar", "Cargo.toml"),
	)
	process.cmd.Dir = repoRoot
	process.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	process.cmd.Env = append(
		os.Environ(),
		"SIDECAR_DATA_DIR="+dataDir,
		"SIDECAR_LISTEN_ADDR="+listenAddr,
	)
	process.cmd.Stdout = &process.output
	process.cmd.Stderr = &process.output
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start live Rust sidecar: %v", err)
	}
	go func() {
		err := process.cmd.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process
}

func (process *sidecarProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-process.done:
		return
	default:
	}
	if err := syscall.Kill(-process.cmd.Process.Pid, syscall.SIGINT); err != nil {
		t.Logf("interrupt live Rust sidecar: %v", err)
	}
	select {
	case <-process.done:
	case <-time.After(5 * time.Second):
		if err := syscall.Kill(-process.cmd.Process.Pid, syscall.SIGKILL); err != nil {
			t.Errorf("kill unresponsive live Rust sidecar: %v", err)
		}
		<-process.done
	}
}

func waitForSidecarHealth(t *testing.T, process *sidecarProcess, sidecarURL string) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(sidecarURL + "/health")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-process.done:
			process.mu.Lock()
			waitErr := process.waitErr
			process.mu.Unlock()
			t.Fatalf("live Rust sidecar exited before health: %v\n%s", waitErr, process.output.String())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("live Rust sidecar did not become healthy\n%s", process.output.String())
}

func getRustVerify(t *testing.T, sidecarURL string) rustVerifyResponse {
	t.Helper()
	response, err := http.Get(sidecarURL + "/events/verify")
	if err != nil {
		t.Fatalf("GET Rust verification: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET Rust verification status = %d", response.StatusCode)
	}
	var verify rustVerifyResponse
	if err := json.NewDecoder(response.Body).Decode(&verify); err != nil {
		t.Fatalf("decode Rust verification: %v", err)
	}
	return verify
}

func getRustEvents(t *testing.T, sidecarURL string) rustEventsResponse {
	t.Helper()
	response, err := http.Get(sidecarURL + "/events")
	if err != nil {
		t.Fatalf("GET Rust events: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET Rust events status = %d", response.StatusCode)
	}
	var events rustEventsResponse
	if err := json.NewDecoder(response.Body).Decode(&events); err != nil {
		t.Fatalf("decode Rust events: %v", err)
	}
	return events
}
