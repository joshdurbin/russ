package state

import (
	"context"
	"fmt"

	"github.com/qmuntal/stateless"
)

// Trigger names for the upgrade lifecycle FSM.
const (
	TriggerAddV8Instance     = "AddV8Instance"
	TriggerFailover          = "Failover"
	TriggerBreakReplication  = "BreakReplication"
	TriggerDestroyV6         = "DestroyV6"
	TriggerAddV8Sentinels    = "AddV8Sentinels"
	TriggerRemoveV6Sentinels = "RemoveV6Sentinels"
)

// Machine wraps a stateless FSM for a cluster's upgrade lifecycle.
// State is persisted to disk after each successful transition.
type Machine struct {
	sm          *stateless.StateMachine
	clusterName string
}

// configure applies the canonical FSM topology to sm. Called by both
// NewMachine and WalkLifecycle so they share one source of truth.
func configure(sm *stateless.StateMachine) {
	sm.Configure(string(StateAllV6)).
		Permit(TriggerAddV8Instance, string(StateMixedVersions))

	sm.Configure(string(StateMixedVersions)).
		Permit(TriggerFailover, string(StateFailoverComplete))

	sm.Configure(string(StateFailoverComplete)).
		Permit(TriggerBreakReplication, string(StateReplicationBroken))

	sm.Configure(string(StateReplicationBroken)).
		Permit(TriggerDestroyV6, string(StateV6Destroyed))

	sm.Configure(string(StateV6Destroyed)).
		Permit(TriggerAddV8Sentinels, string(StateV8SentinelsAdded))

	sm.Configure(string(StateV8SentinelsAdded)).
		Permit(TriggerRemoveV6Sentinels, string(StateDone))

	sm.Configure(string(StateDone))
}

// NewMachine creates a Machine in the persisted state for clusterName,
// defaulting to StateAllV6 if no state file exists.
func NewMachine(clusterName string) (*Machine, error) {
	current, err := Load(clusterName)
	if err != nil {
		return nil, err
	}

	sm := stateless.NewStateMachine(string(current))
	configure(sm)
	return &Machine{sm: sm, clusterName: clusterName}, nil
}

// Transition describes one outgoing edge from a state.
type Transition struct {
	Trigger string
	Dest    State
}

// InitialState is the state a cluster occupies before any upgrade work has
// happened. WalkLifecycle starts its traversal from here.
const InitialState = StateAllV6

// WalkLifecycle introspects the FSM topology by configuring fresh state
// machines, querying their permitted triggers, and simulating each transition
// to discover the destination state. Returns:
//   - states: BFS-ordered list of every reachable state from InitialState
//   - transitions: outgoing edges per state (slice ordered as discovered)
//
// Reads from the same configure() helper that NewMachine uses, so any future
// change to the FSM is automatically reflected without touching this function
// or its callers.
func WalkLifecycle(ctx context.Context) ([]State, map[State][]Transition, error) {
	visited := map[State]bool{}
	var order []State
	transitions := map[State][]Transition{}

	queue := []State{InitialState}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if visited[s] {
			continue
		}
		visited[s] = true
		order = append(order, s)

		probe := stateless.NewStateMachine(string(s))
		configure(probe)
		trigs, err := probe.PermittedTriggersCtx(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("permitted triggers from %s: %w", s, err)
		}

		for _, t := range trigs {
			trigName, ok := t.(string)
			if !ok {
				continue
			}
			// Fresh machine per probe so the previous fire doesn't pollute the
			// next one (stateless mutates state on Fire).
			fire := stateless.NewStateMachine(string(s))
			configure(fire)
			if err := fire.FireCtx(ctx, trigName); err != nil {
				return nil, nil, fmt.Errorf("fire %s from %s: %w", trigName, s, err)
			}
			destAny, err := fire.State(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("read state after %s: %w", trigName, err)
			}
			dest := State(destAny.(string))
			transitions[s] = append(transitions[s], Transition{Trigger: trigName, Dest: dest})
			if !visited[dest] {
				queue = append(queue, dest)
			}
		}
	}

	return order, transitions, nil
}

// Fire runs action and, if it succeeds, advances the state machine by trigger.
// The new state is persisted to disk only after a successful transition.
func (m *Machine) Fire(ctx context.Context, trigger string, action func(context.Context) error) error {
	ok, err := m.sm.CanFireCtx(ctx, trigger)
	if err != nil {
		return err
	}
	if !ok {
		current, _ := m.sm.State(ctx)
		return fmt.Errorf("trigger %q is not valid from state %q", trigger, current)
	}

	if action != nil {
		if err := action(ctx); err != nil {
			return err
		}
	}

	if err := m.sm.FireCtx(ctx, trigger); err != nil {
		return fmt.Errorf("state transition failed: %w", err)
	}

	newState, err := m.sm.State(ctx)
	if err != nil {
		return err
	}
	return Save(m.clusterName, State(newState.(string)))
}

// Current returns the machine's current state.
func (m *Machine) Current(ctx context.Context) (State, error) {
	s, err := m.sm.State(ctx)
	if err != nil {
		return "", err
	}
	return State(s.(string)), nil
}

// CanFire reports whether trigger is permitted from the current state.
func (m *Machine) CanFire(ctx context.Context, trigger string) (bool, error) {
	return m.sm.CanFireCtx(ctx, trigger)
}

// PermittedTriggers returns the list of triggers that can be fired from the current state.
func (m *Machine) PermittedTriggers(ctx context.Context) ([]string, error) {
	triggers, err := m.sm.PermittedTriggersCtx(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(triggers))
	for _, t := range triggers {
		result = append(result, t.(string))
	}
	return result, nil
}
