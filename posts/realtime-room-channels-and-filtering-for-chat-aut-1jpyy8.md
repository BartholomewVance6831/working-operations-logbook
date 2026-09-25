# Realtime Room Channels and Filtering for Chat Authorization in 3 Health Poll Failures

A live health-session poll has one unforgiving constraint: the attendee count must describe who can participate now, not who opened a tab ten minutes ago. **Short answer: use a channel per room as the authorization and presence boundary, enforce membership on the server, and treat reconnect as a new authorization decision.** One shared channel with client-side filtering is not an authorization design. Server-side filtering can be valid, but it needs the same isolated membership state and usually buys complexity before it buys anything useful for this workload.

This choice does not make delivery perfect, and it does not turn presence into durable truth. It makes failure containment legible. At 03:00, I want the page to identify one session whose eligible-voter count diverged from its active leases; a green fleet-wide connection graph tells me very little.

## The incident lesson starts with the page that should fire

Use a bounded incident drill: a clinician opens poll room `session-481`, disconnects on a hospital Wi-Fi transition, and reconnects from the same browser while the old connection has not yet expired. A second participant has two tabs. A third closes a laptop without sending a clean leave. No vendor behavior is assumed here; these are the three lifecycle transitions the design must classify before anyone trusts the number beside "12 responses expected." The tempting implementation keeps one long-lived channel for every session and lets each browser discard events whose `room_id` does not match. It looks economical on a diagram. It also places the authorization boundary after delivery: the browser receives an envelope before deciding whether to show it. Don't do that. A display filter can reduce UI noise, but it cannot establish permission because code controlled by the recipient is already holding the message. A room-scoped channel changes the unit of reasoning. A successful join creates a short-lived presence lease for one authenticated principal, one room, and one connection generation. Reconnect does not revive the old decision; it asks again. A publish checks room membership again rather than inferring permission from possession of a socket. The invariant is compact enough to put in a postmortem action item: **no poll event enters a connection unless the current principal is authorized for that room, and no connection contributes more than one active lease per principal and generation policy.**

That last clause needs a product decision. If two tabs should count as one attendee, deduplicate presence by principal while retaining both delivery connections. If facilitators need to see devices rather than people, count connections and label the metric accordingly. I'm not sure which definition your clinical workflow needs; the poll owner and privacy reviewer have to resolve it before implementation. A dashboard cannot settle a semantic argument.

Then ask the pager question: what page fired? I would alert on a sustained mismatch between authorized active leases and the cohort used to calculate poll completion, scoped by session. I would not page on raw reconnect volume alone. Reconnects can be noisy without changing correctness, while one improperly retained lease can make a small session's completion denominator wrong.

## How should one channel with filtering enforce chat authorization per room?

The phrase "one channel with filtering" hides two materially different designs. Client-side filtering is disqualified for confidential room traffic because delivery precedes the filter. Server-side filtering can enforce the boundary before fan-out, but then the server still maintains an authorization index keyed by principal and room, evaluates it for subscription and publication, and isolates presence calculations. Operationally, that is room membership with a shared transport name.

| Decision point | Channel per room | One channel, server-side filtering | One channel, client-side filtering |
| --- | --- | --- | --- |
| Authorization check | Before room join and again before publish | Before fan-out and again before publish | After delivery; unsuitable as authorization |
| Presence scope | Naturally keyed to the room | Must be projected from filtered server state | Browser-local view cannot be authoritative |
| Failure containment | A bad lease affects one room | A predicate or index error can cross room boundaries | An envelope can reach the wrong recipient |
| Operational cost | More joins and leaves to observe | Fewer logical channel names, more predicate state | Simple server path, unacceptable trust boundary |

For the health-session poll, choose the first column unless measurements show that room churn or subscription cardinality violates a documented platform limit. The important measurement is not a vague "realtime load" chart. Record join decisions, authorization denials, lease generations, expiry lag, duplicate-principal projections, and the room-scoped difference between eligible voters and active participants. Keep identifiers pseudonymous in telemetry, with access and retention set by the system's actual privacy requirements.

Transport and authorization are separate layers. A WebRTC data channel, for example, has configurable delivery characteristics; those settings do not decide who may enter a poll room. If unordered or partially reliable delivery is chosen, the application protocol must tolerate gaps and reordering. The room boundary, sequence rule, and reconnect rule still belong to the application.

Short version: filter before fan-out, never after it.

## The preventative code path is a lease, not a socket flag

A boolean such as `socket.authorized = true` ages badly because it does not say authorized for what, under which policy decision, or before which reconnect. The following Go sketch keeps the transport generic and makes the state transition explicit. `Authorize` represents the application's policy service; `Presence` is a concurrency-safe lease store. Production storage can differ, but the contract should survive that substitution.

```go
package roomauth

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrForbidden = errors.New("room access denied")

type Authorize func(ctx context.Context, principalID, roomID string) (bool, error)

type Lease struct {
	PrincipalID string
	RoomID      string
	Generation  uint64
	ExpiresAt   time.Time
}

type Presence struct {
	mu     sync.Mutex
	leases map[string]Lease
}

func NewPresence() *Presence {
	return &Presence{leases: make(map[string]Lease)}
}

func leaseKey(principalID, roomID string) string {
	return principalID + "\x00" + roomID
}

func (p *Presence) Join(
	ctx context.Context,
	authorize Authorize,
	principalID string,
	roomID string,
	generation uint64,
	now time.Time,
	ttl time.Duration,
) (Lease, error) {
	allowed, err := authorize(ctx, principalID, roomID)
	if err != nil {
		return Lease{}, err
	}
	if !allowed {
		return Lease{}, ErrForbidden
	}

	lease := Lease{
		PrincipalID: principalID,
		RoomID:      roomID,
		Generation:  generation,
		ExpiresAt:   now.Add(ttl),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	key := leaseKey(principalID, roomID)
	current, exists := p.leases[key]
	if !exists || generation >= current.Generation {
		p.leases[key] = lease
	}
	return p.leases[key], nil
}

func (p *Presence) CanPublish(
	ctx context.Context,
	authorize Authorize,
	principalID string,
	roomID string,
) error {
	allowed, err := authorize(ctx, principalID, roomID)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrForbidden
	}
	return nil
}
```

There are deliberate limits to the sketch. It does not claim that process memory is a distributed presence system, and it leaves TTL selection to measured reconnect behavior and the product's definition of "present." It does demonstrate the preventative path: authorize before storing membership, replace an older generation rather than incrementing an unbounded connection counter, and authorize the write separately. The server should also bind `principalID` to authenticated connection context; accepting it from an event payload would hand the decision back to the client.

Test transitions, not screenshots. Start a join, reconnect with a higher generation, deliver a late leave from the lower generation, revoke room access, and attempt a publish. The expected result is that the late leave cannot erase the newer lease, revocation prevents the next publish and reconnect, and expiry removes abandoned presence without treating a missing disconnect frame as evidence of continued attendance. Run the same cases during a rolling deployment because mixed process generations are where hidden local state becomes painfully visible.

The useful event record is small: decision time, pseudonymous principal key, room key, connection generation, policy result, lease expiry, and a reason code such as `membership_revoked`. Do not log message bodies to debug authorization. Correlate these records with the poll's expected-voter calculation, then page on a correctness symptom that an operator can act on.

## When should a team chat avoid channel-per-room presence?

**The catch is that channel per room is not suitable when a participant must continuously observe a very large, rapidly changing room set and the transport imposes a measured subscription ceiling or join latency that breaches the session objective.** In that case, keep server-side authorization and presence indexes per room, but multiplex their already-authorized output over a smaller number of transport streams. The shared stream is an implementation detail, not permission.

Stick with a separate durable log or database when the real requirement is audit, exact vote recovery, or ordered replay. Presence is deliberately transient; stretching it into the source of truth makes expiry and reconnect policy decide history. Likewise, use a media-oriented path when the workload is continuous audio or video rather than small poll and chat events. Your mileage may vary on the crossover point because payload rate, participant fan-out, reconnect patterns, and transport limits are deployment measurements, not universal constants.

Before rollout, run a session-scoped failure exercise with the three cases from the opening and one policy-revocation case. The release gate is not "all sockets connected." It is that unauthorized envelopes remain undelivered, the visible attendee definition matches the documented lease projection, stale generations cannot overwrite current state, and the alert names the affected room. If those statements are hard to prove, the architecture is not ready for the pager.

## References

- W3C, WebRTC 1.0: Real-Time Communication Between Browsers: https://www.w3.org/TR/webrtc/
