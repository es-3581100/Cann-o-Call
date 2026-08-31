package policy

import (
	"errors"
	"fmt"
	"regexp"
)

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|pwd)\s*[:=]`),
	regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?key|secret[_-]?key|auth[_-]?token)\s*[:=]`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`-----BEGIN PRIVATE KEY-----`),
	regexp.MustCompile(`-----BEGIN (RSA|EC|DSA|OPENSSH) PRIVATE KEY-----`),
	regexp.MustCompile(`Bearer\s+[A-Za-z0-9\-._~+/]+=*`),
}

func ScanBytes(data []byte) []string {
	detections := []string{}

	for _, re := range secretPatterns {
		if re.Match(data) {
			detections = append(detections, re.String())
		}
	}

	return detections
}

func RejectIfSecrets(data []byte) error {
	detections := ScanBytes(data)

	if len(detections) > 0 {
		return fmt.Errorf(
			"secret-bearing data rejected by policy: %d secret pattern(s) detected",
			len(detections),
		)
	}

	return nil
}

func MustRejectSecrets(data []byte) {
	if err := RejectIfSecrets(data); err != nil {
		panic(errors.New("secret-bearing data detected"))
	}
}
