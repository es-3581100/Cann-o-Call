package actorstub

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

// Activation is the dormant descriptor + live activation record.
// It retains backward-compatible fields (ID, WorkspaceID, TriggerAction, Path,
// LineageID, Depth, Budget, Status, CreatedAt, ExpiresAt, Notes) and adds
// CHUNK-04 required metadata. JSON remains additive.
// Dormant descriptors are metadata-only; most actors remain dormant. Persistence
// is in-memory only; after process restart ephemeral actors are not resurrected
// and descriptors are rebuilt empty. This limitation is intentional for the
// local dormant runtime (file-backed persistence is future work).
type Activation struct {
	ID              string        `json:"id"`
	WorkspaceID     string        `json:"workspace_id"`
	TriggerAction   string        `json:"trigger_action"`
	Path            string        `json:"path,omitempty"`
	NodeID          string        `json:"node_id,omitempty"`
	LineageID       string        `json:"lineage_id"`
	ParentActorID   string        `json:"parent_actor_id,omitempty"`
	Depth           int           `json:"depth"`
	Budget          int           `json:"budget"`
	RemainingBudget int           `json:"remaining_budget"`
	ActivationCount int           `json:"activation_count"`
	CreatedAt       time.Time     `json:"created_at"`
	LastActivatedAt time.Time     `json:"last_activated_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
	TTL             time.Duration `json:"ttl"`
	Status          string        `json:"status"`
	ContentID       string        `json:"content_id,omitempty"`
	Notes           []string      `json:"notes,omitempty"`
}

// DormantDescriptor aliases Activation for spec language; it is metadata, not
// a permanently running process.
type DormantDescriptor = Activation

// ActorResult is the typed bounded result message produced by an actor.
// Confidence is telemetry only and never authorizes state transitions.
type ActorResult struct {
	ActorID         string    `json:"actor_id"`
	LineageID       string    `json:"lineage_id"`
	NodeID          string    `json:"node_id,omitempty"`
	Status          string    `json:"status"`
	Observations    string    `json:"observations,omitempty"`
	Result          string    `json:"result,omitempty"`
	Confidence      float64   `json:"confidence"`
	EvidenceRefs    []string  `json:"evidence_refs,omitempty"`
	ProposedActions []string  `json:"proposed_actions,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// ProcessMessage is the bounded message delivered to a materialized actor.
type ProcessMessage struct {
	Payload string
	From    string
}

// Config holds hard limits for governed activation. Reuses existing env/config
// patterns with defaults: maxDepth=8, maxBudget=32, maxActive=16, ttl=5m.
type Config struct {
	MaxActive int
	MaxDepth  int
	MaxBudget int
	TTL       time.Duration
}

func DefaultConfig() Config {
	return Config{MaxActive: 16, MaxDepth: 8, MaxBudget: 32, TTL: 5 * time.Minute}
}

// Controller is the Go activation/admission authority facade. Proto.Actor
// provides lifecycle/mailbox/supervision only. Rust remains durable authority.
// Actors may never directly mutate canonical state, bypass Go admission, append
// durable events directly, or spawn unbounded trees.
type Controller struct {
	mu          sync.Mutex
	maxActive   int
	maxDepth    int
	maxBudget   int
	ttl         time.Duration
	system      *actor.ActorSystem
	root        *actor.RootContext
	activations []*Activation
	descriptors map[string]*Activation
	dedup       map[string]time.Time
	live        map[string]*actor.PID
	timers      map[string]*time.Timer
	results     map[string]*ActorResult
	counter     int
	closed      bool
}

// New creates a local dormant runtime with Proto.Actor ActorSystem/Props/PID.
// It preserves the existing constructor signature for server compatibility.
// Env overrides for depth/budget are read via ConfigFromEnv if needed by caller;
// this constructor uses safe defaults for those.
func New(maxActive int, ttl time.Duration) *Controller {
	if maxActive <= 0 {
		maxActive = 16
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	cfg := DefaultConfig()
	cfg.MaxActive = maxActive
	cfg.TTL = ttl
	return NewWithConfig(cfg)
}

// NewWithConfig creates a runtime with explicit limits. No remote/distributed
// features are enabled; ActorSystem is local only.
func NewWithConfig(cfg Config) *Controller {
	if cfg.MaxActive <= 0 {
		cfg.MaxActive = 16
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 8
	}
	if cfg.MaxBudget <= 0 {
		cfg.MaxBudget = 32
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 5 * time.Minute
	}
	sys := actor.NewActorSystem()
	root := actor.NewRootContext(sys, nil)
	return &Controller{
		maxActive:   cfg.MaxActive,
		maxDepth:    cfg.MaxDepth,
		maxBudget:   cfg.MaxBudget,
		ttl:         cfg.TTL,
		system:      sys,
		root:        root,
		activations: []*Activation{},
		descriptors: map[string]*Activation{},
		dedup:       map[string]time.Time{},
		live:        map[string]*actor.PID{},
		timers:      map[string]*time.Timer{},
		results:     map[string]*ActorResult{},
	}
}

// System exposes the underlying ActorSystem for verification that a real
// Proto.Actor runtime is present. No remoting is enabled.
func (c *Controller) System() *actor.ActorSystem { return c.system }

// Root exposes the RootContext for advanced usage if needed.
func (c *Controller) Root() *actor.RootContext { return c.root }

// Config returns current limits.
func (c *Controller) Config() Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Config{MaxActive: c.maxActive, MaxDepth: c.maxDepth, MaxBudget: c.maxBudget, TTL: c.ttl}
}

// Activate is the server-facing facade. It performs governed activation:
// request -> Go validation -> lineage/depth/budget/dedup checks -> ACCEPT|REJECT
// -> if accepted Proto.Actor materialization -> bounded processing -> result
// -> actor stops/passivates. Returns nil only for within-window duplicate
// suppression (to preserve legacy dedup-nil contract).
func (c *Controller) Activate(workspaceID, action, path string) *Activation {
	return c.ActivateWithParent("", workspaceID, action, path)
}

// ActivateWithParent performs lineage-propagated activation. If parentActorID is
// non-empty, child.lineage_id == parent.lineage_id, child.depth == parent.depth+1,
// child.remaining_budget == parent.remaining_budget-1. Root gets new lineage.
func (c *Controller) ActivateWithParent(parentActorID, workspaceID, action, path string) *Activation {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.cleanLocked(now)
	key := strings.Join([]string{workspaceID, action, path}, "\x00")
	// For root activations, global dedup suppresses duplicates.
	// For child activations, lineage-scoped dedup and cycle checks govern; skip global dedup to allow cycle detection.
	if parentActorID == "" {
		if expiresAt, ok := c.dedup[key]; ok && now.Before(expiresAt) {
			return nil
		}
	}
	// Count active for max_active bound
	activeCount := 0
	for _, a := range c.activations {
		if a.Status == "active" {
			activeCount++
		}
	}
	c.counter++
	id := fmt.Sprintf("actor-%06d", c.counter)
	var lineageID string
	var depth int
	var remainingBudget int
	var parentID string
	var activationCount int = 1
	nodeID := workspaceID // subject identity; use workspace as node/subject where path is content
	contentID := path

	if parentActorID != "" {
		parent, ok := c.descriptors[parentActorID]
		if !ok {
			// invalid/missing lineage: reject
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				ParentActorID:   parentActorID,
				LineageID:       "",
				Depth:           0,
				Budget:          0,
				RemainingBudget: 0,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_invalid_lineage",
				Notes:           []string{"parent actor not found or invalid lineage"},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
		if parent.LineageID == "" {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				ParentActorID:   parentActorID,
				LineageID:       "",
				Depth:           0,
				Budget:          0,
				RemainingBudget: 0,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_invalid_lineage",
				Notes:           []string{"parent has invalid lineage"},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
		// Cycle check: same node_id (subject) reactivation without useful state change
		// If child subject == parent subject and same action/path within same lineage, bound recursion.
		if parent.NodeID != "" && nodeID == parent.NodeID && action == parent.TriggerAction && path == parent.Path {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				ParentActorID:   parentActorID,
				LineageID:       parent.LineageID,
				Depth:           parent.Depth + 1,
				Budget:          parent.RemainingBudget - 1,
				RemainingBudget: parent.RemainingBudget - 1,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_cycle",
				Notes:           []string{"cycle/repeated subject bounded"},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
		lineageID = parent.LineageID
		depth = parent.Depth + 1
		remainingBudget = parent.RemainingBudget - 1
		parentID = parentActorID
		activationCount = parent.ActivationCount + 1
		// Enforce lineage-scoped duplicate: if same key already live in this lineage, suppress
		for _, a := range c.activations {
			if a.Status == "active" && a.LineageID == lineageID && a.WorkspaceID == workspaceID && a.TriggerAction == action && a.Path == path {
				return nil
			}
		}
		// Hard limit checks
		if depth > c.maxDepth {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				ParentActorID:   parentID,
				LineageID:       lineageID,
				Depth:           depth,
				Budget:          remainingBudget,
				RemainingBudget: remainingBudget,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_depth",
				Notes:           []string{fmt.Sprintf("depth %d exceeds max_depth %d", depth, c.maxDepth)},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
		if remainingBudget < 0 {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				ParentActorID:   parentID,
				LineageID:       lineageID,
				Depth:           depth,
				Budget:          remainingBudget,
				RemainingBudget: remainingBudget,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_budget_exhausted",
				Notes:           []string{"activation budget exhausted"},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
	} else {
		lineageID = fmt.Sprintf("lineage-%06d", c.counter)
		depth = 1
		remainingBudget = c.maxBudget
		if depth > c.maxDepth {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				LineageID:       lineageID,
				Depth:           depth,
				Budget:          remainingBudget,
				RemainingBudget: remainingBudget,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_depth",
				Notes:           []string{fmt.Sprintf("depth %d exceeds max_depth %d", depth, c.maxDepth)},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
		if remainingBudget < 0 {
			act := &Activation{
				ID:              id,
				WorkspaceID:     workspaceID,
				TriggerAction:   action,
				Path:            path,
				NodeID:          nodeID,
				LineageID:       lineageID,
				Depth:           depth,
				Budget:          remainingBudget,
				RemainingBudget: remainingBudget,
				ActivationCount: activationCount,
				CreatedAt:       now,
				LastActivatedAt: now,
				ExpiresAt:       now.Add(c.ttl),
				TTL:             c.ttl,
				ContentID:       contentID,
				Status:          "rejected_budget_exhausted",
				Notes:           []string{"activation budget exhausted"},
			}
			c.activations = append(c.activations, act)
			c.descriptors[id] = act
			return act
		}
	}

	activation := &Activation{
		ID:              id,
		WorkspaceID:     workspaceID,
		TriggerAction:   action,
		Path:            path,
		NodeID:          nodeID,
		LineageID:       lineageID,
		ParentActorID:   parentID,
		Depth:           depth,
		Budget:          remainingBudget,
		RemainingBudget: remainingBudget,
		ActivationCount: activationCount,
		CreatedAt:       now,
		LastActivatedAt: now,
		ExpiresAt:       now.Add(c.ttl),
		TTL:             c.ttl,
		ContentID:       contentID,
	}

	if activeCount >= c.maxActive {
		activation.Status = "rejected_budget"
		activation.Notes = []string{"actor-count limit reached"}
		c.activations = append(c.activations, activation)
		c.descriptors[id] = activation
		return activation
	}

	activation.Status = "active"
	c.activations = append(c.activations, activation)
	c.descriptors[id] = activation
	c.dedup[key] = activation.ExpiresAt

	// Materialize Proto.Actor only after governed validation passes.
	c.spawnLocked(activation)
	return activation
}

func (c *Controller) spawnLocked(act *Activation) {
	if c.closed || c.system == nil || c.root == nil {
		act.Status = "failed"
		act.Notes = append(act.Notes, "actor system unavailable")
		return
	}
	// Bounded supervision: panic/failure must not crash whole runtime.
	// Use OneForOne with StopDirective (fail-closed), limited restarts.
	strategy := actor.NewOneForOneStrategy(3, 10*time.Second, func(reason interface{}) actor.Directive {
		return actor.StopDirective
	})
	capturedID := act.ID
	props := actor.PropsFromProducer(func() actor.Actor {
		return &dormantActor{id: capturedID, controller: c}
	}, actor.WithSupervisor(strategy))

	// Use anonymous spawn to avoid name collisions; track PID by actor ID.
	pid := c.root.Spawn(props)
	if pid == nil {
		act.Status = "failed"
		act.Notes = append(act.Notes, "spawn failed")
		return
	}
	c.live[act.ID] = pid
	// Deterministic TTL passivation: use AfterFunc to stop/passivate.
	ttl := c.ttl
	if act.TTL > 0 {
		ttl = act.TTL
	}
	idCopy := act.ID
	timer := time.AfterFunc(ttl, func() {
		c.expireActor(idCopy)
	})
	c.timers[act.ID] = timer
}

func (c *Controller) expireActor(actorID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	act, ok := c.descriptors[actorID]
	if !ok {
		return
	}
	if act.Status != "active" {
		return
	}
	act.Status = "expired"
	act.Notes = append(act.Notes, "ttl deterministic stop")
	c.stopLiveLocked(actorID)
	// Clean dedup window for this key so future activations can proceed after expiry.
	key := strings.Join([]string{act.WorkspaceID, act.TriggerAction, act.Path}, "\x00")
	delete(c.dedup, key)
}

func (c *Controller) stopLiveLocked(actorID string) {
	if pid, ok := c.live[actorID]; ok {
		delete(c.live, actorID)
		if timer, ok := c.timers[actorID]; ok {
			timer.Stop()
			delete(c.timers, actorID)
		}
		// Deterministic stop/passivation via Proto.Actor lifecycle primitive.
		if c.root != nil && pid != nil {
			c.root.Stop(pid)
		}
	}
}

// Send delivers a bounded message to a live actor.
func (c *Controller) Send(actorID string, msg interface{}) error {
	c.mu.Lock()
	pid, ok := c.live[actorID]
	act, hasAct := c.descriptors[actorID]
	c.mu.Unlock()
	if !ok || pid == nil {
		return fmt.Errorf("actor %q not live", actorID)
	}
	if hasAct && act.Status != "active" {
		return fmt.Errorf("actor %q status %q not active", actorID, act.Status)
	}
	c.root.Send(pid, msg)
	return nil
}

// Complete marks an actor completed and deterministically stops it.
func (c *Controller) Complete(actorID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	act, ok := c.descriptors[actorID]
	if !ok {
		return
	}
	if act.Status == "active" {
		act.Status = "completed"
		act.Notes = append(act.Notes, "completed -> stop/passivate")
	}
	c.stopLiveLocked(actorID)
	if act != nil {
		key := strings.Join([]string{act.WorkspaceID, act.TriggerAction, act.Path}, "\x00")
		delete(c.dedup, key)
	}
}

// Fail marks failure without crashing system.
func (c *Controller) Fail(actorID string, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	act, ok := c.descriptors[actorID]
	if !ok {
		return
	}
	if act.Status == "active" {
		act.Status = "failed"
		act.Notes = append(act.Notes, reason)
	}
	c.stopLiveLocked(actorID)
}

// RecordResult stores typed bounded actor result. Called by actor after bounded processing.
func (c *Controller) RecordResult(res ActorResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if res.ActorID == "" {
		return
	}
	cp := res
	cp.CreatedAt = time.Now().UTC()
	c.results[res.ActorID] = &cp
	// If actor requested completion, transition status accordingly but keep result.
	if act, ok := c.descriptors[res.ActorID]; ok && act.Status == "active" {
		// Keep active until explicit Complete or TTL; result alone does not auto-complete unless requested.
		// If result status indicates completion, auto-complete.
		if res.Status == "completed" || res.Status == "observation" {
			// Mark completed and stop
			act.Status = "completed"
			act.Notes = append(act.Notes, "result completed -> stop")
			c.stopLiveLocked(res.ActorID)
			key := strings.Join([]string{act.WorkspaceID, act.TriggerAction, act.Path}, "\x00")
			delete(c.dedup, key)
		}
	}
}

// GetResult retrieves last result for actor.
func (c *Controller) GetResult(actorID string) (*ActorResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.results[actorID]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

// LiveCount returns number of currently live PIDs.
func (c *Controller) LiveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.live)
}

// IsLive reports whether actor has live PID.
func (c *Controller) IsLive(actorID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.live[actorID]
	return ok
}

// Descriptor returns dormant descriptor (even if not live).
func (c *Controller) Descriptor(actorID string) (*Activation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.descriptors[actorID]
	if !ok {
		return nil, false
	}
	cp := *a
	return &cp, true
}

// DescriptorCount reports total descriptors tracked.
func (c *Controller) DescriptorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.descriptors)
}

func (c *Controller) cleanLocked(now time.Time) {
	for _, a := range c.activations {
		if a.Status == "active" && now.After(a.ExpiresAt) {
			a.Status = "expired"
			c.stopLiveLocked(a.ID)
			key := strings.Join([]string{a.WorkspaceID, a.TriggerAction, a.Path}, "\x00")
			delete(c.dedup, key)
		}
	}
}

func (c *Controller) clean(now time.Time) {
	for _, a := range c.activations {
		if a.Status == "active" && now.After(a.ExpiresAt) {
			a.Status = "expired"
		}
	}
}

// List returns activations filtered by workspaceID (compat).
func (c *Controller) List(workspaceID string) []*Activation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanLocked(time.Now().UTC())
	out := []*Activation{}
	for _, a := range c.activations {
		if workspaceID == "" || a.WorkspaceID == workspaceID {
			cp := *a
			out = append(out, &cp)
		}
	}
	return out
}

// Shutdown terminates all live actors cleanly via Proto.Actor lifecycle.
// After shutdown, no ephemeral actors remain and system is closed.
func (c *Controller) Shutdown() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	// Stop timers and collect pids to stop after unlock to avoid deadlock with callbacks
	pids := make([]*actor.PID, 0, len(c.live))
	for _, pid := range c.live {
		pids = append(pids, pid)
	}
	for _, t := range c.timers {
		t.Stop()
	}
	c.live = map[string]*actor.PID{}
	c.timers = map[string]*time.Timer{}
	c.mu.Unlock()

	for _, pid := range pids {
		if c.root != nil {
			c.root.Stop(pid)
		}
	}
	// Give mailbox a moment to process Stop
	time.Sleep(20 * time.Millisecond)
	if c.system != nil {
		c.system.Shutdown()
	}
}

func (c *Controller) handleStopped(actorID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// If still marked active but actor stopped (e.g., completed), passivate.
	if act, ok := c.descriptors[actorID]; ok && act.Status == "active" {
		act.Status = "passivated"
		act.Notes = append(act.Notes, "stopped/passivated")
		delete(c.live, actorID)
		if timer, ok := c.timers[actorID]; ok {
			timer.Stop()
			delete(c.timers, actorID)
		}
		key := strings.Join([]string{act.WorkspaceID, act.TriggerAction, act.Path}, "\x00")
		delete(c.dedup, key)
	} else {
		delete(c.live, actorID)
		if timer, ok := c.timers[actorID]; ok {
			timer.Stop()
			delete(c.timers, actorID)
		}
	}
}

func (c *Controller) recordFailure(actorID string, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if act, ok := c.descriptors[actorID]; ok {
		if act.Status == "active" {
			act.Status = "failed"
			act.Notes = append(act.Notes, reason)
		}
		delete(c.live, actorID)
		if timer, ok := c.timers[actorID]; ok {
			timer.Stop()
			delete(c.timers, actorID)
		}
	}
}

// dormantActor is the Proto.Actor-backed local runtime actor. It never directly
// mutates canonical state or appends durable events; it only emits proposals
// via RecordResult for Go admission.
type dormantActor struct {
	id         string
	controller *Controller
}

func (a *dormantActor) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
	case *actor.Stopped:
		a.controller.handleStopped(a.id)
	case *ProcessMessage:
		func() {
			defer func() {
				if r := recover(); r != nil {
					a.controller.recordFailure(a.id, fmt.Sprintf("panic: %v", r))
					ctx.Stop(ctx.Self())
				}
			}()
			if msg.Payload == "panic" {
				panic("intentional actor panic for supervision test")
			}
			// Bounded processing: emit typed result.
			act, ok := a.controller.Descriptor(a.id)
			lineage := ""
			nodeID := ""
			if ok {
				lineage = act.LineageID
				nodeID = act.NodeID
			}
			res := ActorResult{
				ActorID:         a.id,
				LineageID:       lineage,
				NodeID:          nodeID,
				Status:          "observation",
				Observations:    msg.Payload,
				Result:          "processed:" + msg.Payload,
				Confidence:      0.42, // telemetry only
				EvidenceRefs:    []string{"evidence://" + a.id},
				ProposedActions: []string{"no_op"},
				CreatedAt:       time.Now().UTC(),
			}
			a.controller.RecordResult(res)
			// Deterministic stop after bounded processing.
			ctx.Stop(ctx.Self())
		}()
	case string:
		// For tests that send raw string as panic trigger
		if msg == "panic" {
			panic("string panic")
		}
		act, ok := a.controller.Descriptor(a.id)
		lineage := ""
		nodeID := ""
		if ok {
			lineage = act.LineageID
			nodeID = act.NodeID
		}
		res := ActorResult{
			ActorID:      a.id,
			LineageID:    lineage,
			NodeID:       nodeID,
			Status:       "observation",
			Observations: msg,
			Result:       "processed:" + msg,
			Confidence:   0.42,
			CreatedAt:    time.Now().UTC(),
		}
		a.controller.RecordResult(res)
		ctx.Stop(ctx.Self())
	}
}
