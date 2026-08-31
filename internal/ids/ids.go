package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func New(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)

	return fmt.Sprintf(
		"%s-%s-%s",
		prefix,
		time.Now().UTC().Format("20060102150405"),
		hex.EncodeToString(b),
	)
}
