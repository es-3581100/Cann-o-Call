package workspace

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
)

type QuarantinedBlob struct {
	ID           string `json:"id"`
	OriginalPath string `json:"original_path"`
	SafePath     string `json:"safe_path"`
	TargetPath   string `json:"target_path,omitempty"`
	Reason       string `json:"reason"`
	Status       string `json:"status"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
	DecidedAt    string `json:"decided_at,omitempty"`
	ReceiptID    string `json:"receipt_id,omitempty"`
	Data         []byte `json:"-"`
}

func NewQuarantineID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)

	return fmt.Sprintf("quarantine-%s", hex.EncodeToString(b))
}

func DeterministicQuarantineID(workspaceID, originalPath, sha string) string {
	h := sha256.Sum256([]byte(workspaceID + "\x00" + originalPath + "\x00" + sha))

	return fmt.Sprintf("quarantine-%s", hex.EncodeToString(h[:8]))
}

func SafeQuarantinePath(original string, id string, files map[string]*File) string {
	base := path.Base(path.Clean("/" + original))

	if base == "" || base == "." || base == ".." {
		base = "file"
	}

	candidate := path.Join("quarantine", fmt.Sprintf("%s-%s", id, base))

	if _, ok := files[candidate]; !ok {
		return candidate
	}

	for i := 1; ; i++ {
		alt := path.Join("quarantine", fmt.Sprintf("%s-%d-%s", id, i, base))

		if _, ok := files[alt]; !ok {
			return alt
		}
	}
}
