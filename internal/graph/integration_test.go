package graph

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"flatten-workspace/internal/actorstub"
	"flatten-workspace/internal/eventlog"
	"flatten-workspace/internal/ingest"
	"flatten-workspace/internal/transition"
	"flatten-workspace/internal/workspace"
)

type mockWriter struct {
	events []eventlog.Event
	seq    int64
}

func hashEventForTest(ev eventlog.Event) string {
	clone := ev
	clone.Hash = ""
	clone.RustAck = nil
	b, _ := json.Marshal(clone)
	return ingest.ComputeSHA256(b)
}

func (w *mockWriter) Append(ev eventlog.Event) (eventlog.Event, error) {
	w.seq++
	ev.Seq = w.seq
	ev.PrevHash = "genesis"
	if w.seq > 1 && len(w.events) > 0 {
		ev.PrevHash = w.events[len(w.events)-1].Hash
	}
	ev.Hash = hashEventForTest(ev)
	ack := eventlog.RustAcknowledgement{ID: ev.ID, Sequence: ev.Seq, Hash: ingest.ComputeSHA256([]byte("rust-" + ev.ID))}
	// Ensure ack hash is valid sha256 hex
	if len(ack.Hash) != 64 {
		ack.Hash = ingest.ComputeSHA256([]byte(ack.Hash))
	}
	ev.RustAck = &ack
	w.events = append(w.events, ev)
	return ev, nil
}

func TestCH05_16_FailureLeavesProjectionUnchanged(t *testing.T) {
	w := &mockWriter{}
	auth := transition.New(w, nil)
	store := NewStore("wsFail")
	pkt1, _ := ingest.IngestBytes("wsFail", "good.txt", "", []byte("good"), "good.txt")
	at1, err := ingest.ProposeIngest(auth, *pkt1)
	if err != nil {
		t.Fatalf("ProposeIngest good: %v", err)
	}
	p1, _ := ingest.PacketFromAccepted(at1)
	if _, _, err := store.Apply(*p1); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	hashBefore := store.Graph().Hash
	projBefore := auth.Projection()

	badPkt := ingest.SourcePacket{}
	_, err = ingest.ProposeIngest(auth, badPkt)
	if err == nil {
		t.Fatalf("expected error for bad packet")
	}
	projAfter := auth.Projection()
	if projBefore.Hash != projAfter.Hash || projBefore.Version != projAfter.Version {
		t.Fatalf("projection changed on failure: before %v after %v", projBefore, projAfter)
	}
	if store.Graph().Hash != hashBefore {
		t.Fatalf("graph changed on failure")
	}
}

func TestCH05_17_RustUnavailableFailClosed(t *testing.T) {
	dir := t.TempDir()
	svc, err := eventlog.New(dir, "")
	if err != nil {
		t.Fatalf("eventlog.New: %v", err)
	}
	auth, err := transition.NewFromEventLog(svc, nil)
	if err != nil {
		t.Fatalf("NewFromEventLog: %v", err)
	}
	pkt, _ := ingest.IngestBytes("wsRust", "file.txt", "", []byte("data"), "file.txt")
	_, err = ingest.ProposeIngest(auth, *pkt)
	if err == nil {
		t.Fatalf("expected durable failure when Rust unavailable")
	}
	if transition.Class(err) != transition.Durable {
		t.Fatalf("expected Durable class, got %v (%v)", transition.Class(err), err)
	}
	if auth.Projection().Version != 0 {
		t.Fatalf("projection should not advance on durable failure")
	}
	ev := eventlog.Event{ID: "test", Seq: 1, Type: "transition.authority.accepted", Hash: ingest.ComputeSHA256([]byte("x")), RustAck: nil}
	if err := eventlog.ValidateRustAcknowledgement(ev); err == nil {
		t.Fatalf("missing rust ack should fail")
	}
}

func TestCH05_18_RestartReplayReproducesGraph(t *testing.T) {
	w := &mockWriter{}
	auth := transition.New(w, nil)
	store := NewStore("wsReplay")
	for _, p := range []string{"a.txt", "b.txt"} {
		pkt, _ := ingest.IngestBytes("wsReplay", p, "", []byte("content-"+p), p)
		at, err := ingest.ProposeIngest(auth, *pkt)
		if err != nil {
			t.Fatalf("ProposeIngest %s: %v", p, err)
		}
		pRec, _ := ingest.PacketFromAccepted(at)
		if _, _, err := store.Apply(*pRec); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	hashLive := store.Graph().Hash
	cp := auth.Checkpoint()
	history := w.events
	auth2, err := transition.Rebuild(history, w, nil)
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if auth2.Projection().Hash != cp.State.Hash {
		t.Fatalf("rebuilt projection hash mismatch")
	}
	packetsRebuilt, _ := ingest.PacketsFromHistory(cp.Accepted)
	g2, _ := Build(packetsRebuilt)
	if g2.Hash != hashLive {
		t.Fatalf("replay graph hash mismatch %s vs %s", g2.Hash, hashLive)
	}
	store2 := NewStore("wsReplay")
	if _, err := store2.RebuildFromAccepted(cp.Accepted); err != nil {
		t.Fatalf("RebuildFromAccepted: %v", err)
	}
	if store2.Graph().Hash != hashLive {
		t.Fatalf("store replay hash mismatch")
	}
}

func TestCH05_19_GraphDoesNotActivateActors(t *testing.T) {
	ctrl := actorstub.New(16, 0)
	if ctrl == nil {
		t.Fatalf("controller nil")
	}
	defer ctrl.Shutdown()
	pkt, _ := ingest.IngestBytes("wsActor", "file.txt", "", []byte("hello"), "file.txt")
	g, _ := Build([]ingest.SourcePacket{*pkt})
	_ = g
	if ctrl.LiveCount() != 0 {
		t.Fatalf("graph build should not activate actors, liveCount %d", ctrl.LiveCount())
	}
	if len(ctrl.List("wsActor")) != 0 {
		t.Fatalf("no actors should have been created")
	}
	w := &mockWriter{}
	auth := transition.New(w, nil)
	if _, err := ingest.ProposeIngest(auth, *pkt); err != nil {
		t.Fatalf("ProposeIngest: %v", err)
	}
	if ctrl.LiveCount() != 0 {
		t.Fatalf("ingest should not activate actors")
	}
}

func TestCH05_20_FlattenWorkspaceV1Compatibility(t *testing.T) {
	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	fw, _ := zw.Create("root/file.txt")
	fw.Write([]byte("hello"))
	fw2, _ := zw.Create("root/nested/data.json")
	fw2.Write([]byte(`{"k":"v"}`))
	zw.Close()

	ws, err := workspace.FromZipBytes(buf.Bytes(), "test.zip")
	if err != nil {
		t.Fatalf("FromZipBytes: %v", err)
	}
	env, err := ws.ToEnvelope()
	if err != nil {
		t.Fatalf("ToEnvelope: %v", err)
	}
	if env.Format != workspace.FormatV1 {
		t.Fatalf("format not v1")
	}
	packets, quarantined, err := ingest.IngestWorkspace(ws)
	if err != nil {
		t.Fatalf("IngestWorkspace: %v", err)
	}
	if len(quarantined) != 0 {
		t.Fatalf("unexpected quarantine")
	}
	if len(packets) != 2 {
		t.Fatalf("expected 2 packets got %d", len(packets))
	}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ws2, err := workspace.Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	packets2, _, err := ingest.IngestWorkspace(ws2)
	if err != nil {
		t.Fatalf("IngestWorkspace2: %v", err)
	}
	if len(packets2) != len(packets) {
		t.Fatalf("packet count mismatch after round-trip")
	}
	g1, _ := Build(packets)
	g2, _ := Build(packets2)
	if g1.Hash != g2.Hash {
		t.Fatalf("graph hash mismatch after v1 round-trip")
	}
	buf2 := new(bytes.Buffer)
	zw2 := zip.NewWriter(buf2)
	zw2.Create("../evil.txt")
	zw2.Close()
	wsEvil, _ := workspace.FromZipBytes(buf2.Bytes(), "evil.zip")
	if wsEvil.Source.UnsafeEntryCount != 1 {
		t.Fatalf("evil count mismatch")
	}
	packetsEvil, _, _ := ingest.IngestWorkspace(wsEvil)
	if len(packetsEvil) != 0 {
		t.Fatalf("evil should produce 0 packets")
	}
}

func TestCH05_DeterministicGraphRebuildEmpty(t *testing.T) {
	g1, _ := Build(nil)
	g2, _ := Build([]ingest.SourcePacket{})
	if g1.Hash != g2.Hash {
		t.Fatalf("empty graph hash not deterministic")
	}
}

func TestCH05_SnapshotRebuildSameGraph(t *testing.T) {
	w := &mockWriter{}
	auth := transition.New(w, nil)
	store := NewStore("wsSnap")
	for i := 0; i < 3; i++ {
		name := filepath.Join("a", string(rune('a'+i))+".txt")
		pkt, _ := ingest.IngestBytes("wsSnap", name, "", []byte("content"), name)
		at, _ := ingest.ProposeIngest(auth, *pkt)
		pRec, _ := ingest.PacketFromAccepted(at)
		if _, _, err := store.Apply(*pRec); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	hashBefore := store.Graph().Hash
	cp := auth.Checkpoint()
	store2 := NewStore("wsSnap")
	if _, err := store2.RebuildFromAccepted(cp.Accepted); err != nil {
		t.Fatalf("RebuildFromAccepted: %v", err)
	}
	if store2.Graph().Hash != hashBefore {
		t.Fatalf("hash mismatch after discard/rebuild")
	}
}
