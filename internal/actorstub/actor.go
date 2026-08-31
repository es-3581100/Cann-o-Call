package actorstub

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Activation struct {
	ID            string    `json:"id"`
	WorkspaceID   string    `json:"workspace_id"`
	TriggerAction string    `json:"trigger_action"`
	Path          string    `json:"path,omitempty"`
	LineageID     string    `json:"lineage_id"`
	Depth         int       `json:"depth"`
	Budget        int       `json:"budget"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Notes         []string  `json:"notes,omitempty"`
}

type Controller struct {
	mu          sync.Mutex
	maxActive   int
	ttl         time.Duration
	activations []*Activation
	dedup       map[string]time.Time
	counter     int
}

func New(maxActive int, ttl time.Duration) *Controller {
	if maxActive <= 0 {
		maxActive = 16
	}

	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	return &Controller{
		maxActive:   maxActive,
		ttl:         ttl,
		activations: []*Activation{},
		dedup:       map[string]time.Time{},
	}
}

func (c *Controller) Activate(workspaceID, action, path string) *Activation {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()
	c.clean(now)

	key := strings.Join([]string{workspaceID, action, path}, "\x00")

	if expiresAt, ok := c.dedup[key]; ok && now.Before(expiresAt) {
		return nil
	}

	activeCount := 0
	for _, a := range c.activations {
		if a.Status == "active" {
			activeCount++
		}
	}

	c.counter++

	id := fmt.Sprintf("actor-%06d", c.counter)
	lineage := fmt.Sprintf("lineage-%06d", c.counter)

	activation := &Activation{
		ID:            id,
		WorkspaceID:   workspaceID,
		TriggerAction: action,
		Path:          path,
		LineageID:     lineage,
		Depth:         1,
		Budget:        1,
		CreatedAt:     now,
		ExpiresAt:     now.Add(c.ttl),
	}

	if activeCount >= c.maxActive {
		activation.Status = "rejected_budget"
		activation.Notes = []string{"actor-count limit reached"}
		c.activations = append(c.activations, activation)
		return activation
	}

	activation.Status = "active"

	c.activations = append(c.activations, activation)
	c.dedup[key] = activation.ExpiresAt

	return activation
}

func (c *Controller) clean(now time.Time) {
	for _, a := range c.activations {
		if a.Status == "active" && now.After(a.ExpiresAt) {
			a.Status = "expired"
		}
	}
}

func (c *Controller) List(workspaceID string) []*Activation {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clean(time.Now().UTC())

	out := []*Activation{}

	for _, a := range c.activations {
		if workspaceID == "" || a.WorkspaceID == workspaceID {
			out = append(out, a)
		}
	}

	return out
}
