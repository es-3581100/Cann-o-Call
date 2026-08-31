package receipts

import (
	"testing"
)

func TestReceiptChainAndVerify(t *testing.T) {
	dir := t.TempDir()

	s, err := New(dir)
	if err != nil {
		t.Fatalf("create receipt service: %v", err)
	}

	if _, err := s.Save(Receipt{
		ID:              "receipt-chain-0001",
		Action:          "test.one",
		Objective:       "First chained receipt",
		AuthoritySource: "test",
		EventID:         "event-chain-0001",
	}); err != nil {
		t.Fatalf("save first receipt: %v", err)
	}

	if _, err := s.Save(Receipt{
		ID:              "receipt-chain-0002",
		Action:          "test.two",
		Objective:       "Second chained receipt",
		AuthoritySource: "test",
		EventID:         "event-chain-0002",
	}); err != nil {
		t.Fatalf("save second receipt: %v", err)
	}

	result := s.Verify()

	ok, _ := result["ok"].(bool)
	if !ok {
		t.Fatalf("expected receipt chain to verify, got: %+v", result)
	}

	chainedChecked, _ := result["chained_checked"].(int)
	if chainedChecked != 2 {
		t.Fatalf("expected 2 chained receipts, got %d", chainedChecked)
	}
}
