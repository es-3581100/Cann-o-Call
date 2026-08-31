package policy

import "testing"

func TestRejectIfSecretsDetectsPassword(t *testing.T) {
	err := RejectIfSecrets([]byte("password=abc123"))
	if err == nil {
		t.Fatal("expected password pattern to be rejected")
	}
}

func TestRejectIfSecretsDetectsAWSKey(t *testing.T) {
	err := RejectIfSecrets([]byte("key = AKIAABCDEFGHIJKLMNOP"))
	if err == nil {
		t.Fatal("expected AWS key pattern to be rejected")
	}
}

func TestRejectIfSecretsAllowsNormalContent(t *testing.T) {
	err := RejectIfSecrets([]byte("status: active\nphase: implementation"))
	if err != nil {
		t.Fatalf("expected normal content to pass, got: %v", err)
	}
}
