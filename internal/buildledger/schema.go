package buildledger

import (
	"errors"
	"fmt"
	"strings"
)

func ValidateDocument(kind string, doc map[string]any) error {
	switch kind {
	case "state":
		return ValidateState(doc)
	case "run":
		return ValidateRun(doc)
	case "receipt":
		return ValidateReceipt(doc)
	case "verification":
		return ValidateVerification(doc)
	case "event":
		return ValidateEvent(doc)
	default:
		return nil
	}
}

func ValidateState(doc map[string]any) error {
	if doc == nil {
		return errors.New("state document must be a YAML map")
	}

	if err := requireFields(doc, "id", "type", "revision", "status"); err != nil {
		return err
	}

	return requireString(doc, "id", "type", "status")
}

func ValidateRun(doc map[string]any) error {
	if doc == nil {
		return errors.New("run document must be a YAML map")
	}

	if err := requireFields(doc, "id", "type", "status", "objective"); err != nil {
		return err
	}

	return requireString(doc, "id", "type", "status", "objective")
}

func ValidateReceipt(doc map[string]any) error {
	if doc == nil {
		return errors.New("receipt document must be a YAML map")
	}

	if err := requireFields(doc, "id", "type", "status"); err != nil {
		return err
	}

	return requireString(doc, "id", "type", "status")
}

func ValidateVerification(doc map[string]any) error {
	if doc == nil {
		return errors.New("verification document must be a YAML map")
	}

	if err := requireFields(doc, "id", "type", "status"); err != nil {
		return err
	}

	if err := requireString(doc, "id", "type", "status"); err != nil {
		return err
	}

	if checks, ok := doc["checks"]; ok {
		if _, ok := checks.(map[string]any); !ok {
			return errors.New("checks must be a map")
		}
	}

	return nil
}

func ValidateEvent(doc map[string]any) error {
	if doc == nil {
		return errors.New("event document must be a JSON object")
	}

	if err := requireFields(doc, "id", "type", "status", "event"); err != nil {
		return err
	}

	return requireString(doc, "id", "type", "status", "event")
}

func requireFields(doc map[string]any, fields ...string) error {
	missing := []string{}

	for _, field := range fields {
		if _, ok := doc[field]; !ok {
			missing = append(missing, field)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}

	return nil
}

func requireString(doc map[string]any, fields ...string) error {
	for _, field := range fields {
		v, ok := doc[field]
		if !ok {
			return fmt.Errorf("missing required field %q", field)
		}

		s, ok := v.(string)
		if !ok || s == "" {
			return fmt.Errorf("field %q must be a non-empty string", field)
		}
	}

	return nil
}
