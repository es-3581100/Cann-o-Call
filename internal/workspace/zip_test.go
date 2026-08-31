package workspace

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestFromZipBytesSafe(t *testing.T) {
	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)

	fw, err := zw.Create("root/hello.txt")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}

	if _, err := fw.Write([]byte("hello\n")); err != nil {
		t.Fatalf("zip write: %v", err)
	}

	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}

	ws, err := FromZipBytes(buf.Bytes(), "test.zip")
	if err != nil {
		t.Fatalf("FromZipBytes failed: %v", err)
	}

	if ws.FileCount != 1 {
		t.Fatalf("expected 1 file, got %d", ws.FileCount)
	}

	if ws.DirectoryCount != 1 {
		t.Fatalf("expected 1 directory, got %d", ws.DirectoryCount)
	}

	if ws.Source.UnsafeEntryCount != 0 {
		t.Fatalf("expected 0 unsafe entries, got %d", ws.Source.UnsafeEntryCount)
	}

	if _, ok := ws.Files["root/hello.txt"]; !ok {
		t.Fatal("expected root/hello.txt to exist")
	}

	env, err := ws.ToEnvelope()
	if err != nil {
		t.Fatalf("ToEnvelope failed: %v", err)
	}

	if env.Tree["root"] == nil {
		t.Fatal("expected envelope tree to contain root")
	}
}

func TestFromZipBytesRejectsTraversal(t *testing.T) {
	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)

	_, err := zw.Create("../evil.txt")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}

	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}

	ws, err := FromZipBytes(buf.Bytes(), "evil.zip")
	if err != nil {
		t.Fatalf("FromZipBytes failed: %v", err)
	}

	if ws.FileCount != 0 {
		t.Fatalf("expected 0 files, got %d", ws.FileCount)
	}

	if ws.Source.UnsafeEntryCount != 1 {
		t.Fatalf("expected 1 unsafe entry, got %d", ws.Source.UnsafeEntryCount)
	}
}
