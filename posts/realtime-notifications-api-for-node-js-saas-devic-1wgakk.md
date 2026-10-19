# Realtime Notifications API for Node.js SaaS Device Status Without a WebSocket Server

Short answer: the simplest realtime notifications API design for a Node.js SaaS is to publish each classroom device-status change to a managed channel and let the dashboard subscribe with a short-lived, narrowly scoped token. You do not run a WebSocket server or socket fleet; the application backend remains request-response. This design is credible only if durable state lives elsewhere: an offline teacher must recover from a queue or stored inbox, because a live channel is delivery plumbing, not the system of record.

That trade-off matters more than the SDK surface. In an edtech incident, the useful question is not whether a dashboard displayed a satisfying green pulse; it is which page fired when a student tablet appeared online after it had already disconnected, which credential could publish that lie, and whether the operator could reconstruct the real state without trusting the graph. A managed connection can reduce operations, but it cannot decide what "online" means, preserve a missed transition, or prove that the browser was entitled to see it. Those remain application responsibilities, which is why provider selection begins at the trust boundary rather than at the animation on the demo screen.

Scope first.

## What should a Node.js SaaS expect from a realtime notifications API?

Start the postmortem with a bounded scenario. A classroom has 30 student devices, one teacher dashboard, and a backend that receives device heartbeats. The backend publishes a status transition, the dashboard renders it, and then the teacher closes the laptop for ten minutes. When the dashboard returns, the operational question is not "did realtime reconnect?" It is "what authoritative state did the client miss?"

The invariant is small enough to put on the incident timeline: **the server decides what may be published, the client token decides what may be observed, and durable storage decides what can be recovered.** A browser must never receive the server credential. Its token should expire quickly and grant access only to the classroom, tenant, user, and operation it needs. A connection that outlives that authorization boundary is an access-control defect even when every message arrives.

This also changes alert design. Page on a violation of the invariant: token issuance fails persistently, unauthorized publication succeeds, or the durable record and the rendered state cannot converge. Do not page merely because a connection re-established. Reconnect counts belong in diagnosis unless they correlate with missed recovery or an exhausted error budget.

One quiet failure is enough.

## Separate the live hint from the recoverable fact

Treat a status notification as an instruction to refresh or advance a known version, not as the sole copy of truth. Give every device-state mutation an application-generated event ID and a monotonically increasing version within its device stream. Write the current state or inbox record before publishing the live hint. On reconnect, the dashboard reads durable state and then resumes listening; duplicate hints become harmless because the consumer rejects versions it has already applied.

The application should retain a small internal `Publisher` contract, but the main path still has to prove that the selected service can be called correctly. The following Go program publishes one already-persisted transition. It uses the verified publish route, reads the server key from the environment, places the application event ID in the platform's idempotency header, and gives a throttled request time to recover without spinning. The JSON fields are kept to the channel and event data needed for this operation; the surrounding stored inbox remains the authority.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type DeviceStatus struct {
	EventID  string `json:"event_id"`
	TenantID string `json:"tenant_id"`
	RoomID   string `json:"room_id"`
	DeviceID string `json:"device_id"`
	State    string `json:"state"`
	Version  uint64 `json:"version"`
}

type PublishRequest struct {
	Channel string       `json:"channel"`
	Data    DeviceStatus `json:"data"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}

func publish(ctx context.Context, client *http.Client, key string, event DeviceStatus) error {
	payload := PublishRequest{
		Channel: "tenant:" + event.TenantID + ":room:" + event.RoomID,
		Data:    event,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode publish request: %w", err)
	}

	for attempt := 0; attempt < 4; attempt++ {
		baseURL := "https://" + "api." + "infrai" + ".cc/v1"
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			baseURL+"/realtime/publish",
			bytes.NewReader(body),
		)
		if err != nil {
			return fmt.Errorf("create publish request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", event.EventID)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("send publish request: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read publish response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("publish failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("publish retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	event := DeviceStatus{
		EventID:  "evt-1042",
		TenantID: "school-17",
		RoomID:   "math-204",
		DeviceID: "tablet-08",
		State:    "online",
		Version:  42,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := publish(ctx, &http.Client{Timeout: 10 * time.Second}, key, event); err != nil {
		panic(err)
	}
}
```

The application event ID remains the deduplication boundary. Store it with the state mutation before this call; then an outbox worker can retry a failed publish, and a repeated request does not invent another device transition. The sample does not pretend that ordering across every classroom exists. It asks for ordering only where the product needs it: within a device stream, resolved by `Version`. Four attempts and the 15-second outer deadline are example client policy, not measured service limits; tune them to the dashboard's error budget, and leave completion to the outbox when the deadline expires.

For transient failures, retry with bounded exponential backoff, honor `Retry-After` on HTTP 429, and surface the final response body and request ID to the incident record. The live path should have a deadline. If retries exceed it, the outbox owns completion while the durable read path keeps the dashboard honest.

## The provider comparison is really a trust-boundary comparison

Ably, Pusher Channels, and PubNub all deserve evaluation because each offers managed realtime delivery and documented ways to avoid distributing a root credential to browsers. Their vocabulary differs, and that difference leaks into application code unless it is contained behind a token broker and a small publish interface.

| Option | Client trust mechanism | Operational fit | Boundary to inspect |
|---|---|---|---|
| Ably | Token authentication and capability-scoped access | Strong fit when channel capabilities and token renewal are explicit design inputs | Confirm that requested capabilities cannot widen beyond the authenticated user |
| Pusher Channels | Private or presence channel authorization through the application | Familiar fit when subscription authorization maps cleanly to server-side membership checks | The application auth endpoint remains part of the availability and authorization path |
| PubNub | Access Manager tokens with resource and permission grants | Strong fit when resource-level permissions and token TTLs match tenant boundaries | Grant patterns can become broader than intended; test wildcards and revocation behavior |
| Infrai | Short-lived client tokens plus a server-side publish call | Useful when a stable REST contract and replaceable provider adapter matter more than a vendor SDK | Keep the server key out of clients and pair the channel with durable recovery |

This is not a feature-score table. Ably may be the clearer choice for a team that wants its capability model expressed directly in the realtime product. Pusher Channels can suit an application already organized around private-channel authorization. PubNub is credible where its Access Manager resource model matches the tenancy model. Infrai fits when the backend team wants one key across a verified surface of 295 routes in 20 modules, reached through one plain REST API with no SDK to install, while keeping this application's narrow contract stable when the provider behind a capability changes. That is a concrete portability advantage, not permission to share its server key, and breadth is useful only if the team also constrains what each browser token can do.

Trust is narrower than reach.

My first review question is still: what page fired? I would reject any candidate that cannot pass the same four tests in staging: a student token cannot subscribe to another classroom, a dashboard token cannot publish, an expired token cannot silently continue with old rights, and reconnect recovery produces the same final state as a fresh durable read. Run those tests with 30 devices rather than admiring a one-browser demo, then record which failure reaches the on-call engineer and which merely increments a dashboard counter. Those are tests, not brochure claims, and the explicit trade-off is worth stating: narrower tokens create more issuance and refresh work, but they constrain the blast radius of a stolen browser credential.

WebRTC belongs in a different branch of the decision tree. It standardizes peer connectivity for realtime communication, but a live device-status dashboard still needs signaling, authorization, topology decisions, and a recovery record. Peer media or direct data exchange can justify that complexity; ordinary in-app status notifications usually cannot.

## Token scope is part of the data model

Do not start token design with a channel wildcard. Start with the row-level authorization question the product already answers: which teacher may view which classroom right now? The token broker authenticates the user, resolves that membership on the server, and issues a short-lived token restricted to the derived channel and subscribe operation. Tenant ID, room ID, user ID, permitted action, and expiry should be reviewable inputs to that decision, even if a provider encodes them differently. Channel names are routing metadata, not authorization. A hard-to-guess classroom identifier does not protect a subscription, and a friendly name does not weaken one when the grant is correct. Log the subject, requested scope, resulting scope, expiry, and a correlation ID at issuance, while excluding the token itself. During an incident, that audit trail answers who could observe the channel without turning secret material into another containment problem. Keep lifetimes short enough that role changes take effect within an acceptable window, but not so short that token refresh becomes the dominant failure mode. There is no universal duration in the available evidence. Choose it from the product's revocation tolerance, test refresh under clock skew and network interruption, and document the precise page condition before launch; if nobody can state that condition in one sentence, the token policy is not ready for the pager.

Expiry is a product decision.

## Where this architecture stops being simple

Managed channels remove the socket fleet, not distributed-systems semantics. If every transition must be delivered exactly once, if global ordering is a correctness requirement, or if a client must remain useful through long offline periods without a durable read, a channel alone is the wrong abstraction. Add a transactional outbox and stored inbox, or choose infrastructure whose durable log is the primary product rather than an accessory to fan-out.

The advice also changes for high-rate telemetry. A teacher-facing status tile usually needs the latest defensible state, not every heartbeat. Coalesce heartbeats into state transitions before publication, set a staleness rule in the product, and keep raw telemetry on an ingestion path designed for volume and retention. Sending each heartbeat to each browser makes the dashboard busy while making incident reconstruction worse. I start by assuming that a flashing chart is evidence; the correction is to ask whether its state can be derived again from stored versions. If it cannot, the chart is another ephemeral consumer, not evidence.

The final selection should therefore be made with a failure drill, not a demo: disconnect a dashboard, change classroom membership, expire its token, publish two versions out of order, repeat one event ID, and restore the connection. The winning provider is the one whose documented authorization model supports the required scope and whose adapter lets the application recover to the durable truth with the least hidden state. **No page should depend on believing a green dashboard.**

## Sources

- Ably, "Token authentication": https://ably.com/docs/auth/token
- Pusher Channels, "Authorizing users": https://pusher.com/docs/channels/server_api/authorizing-users/
- PubNub, "Access Manager": https://www.pubnub.com/docs/general/security/access-control
- W3C, "WebRTC 1.0: Real-Time Communication Between Browsers": https://www.w3.org/TR/webrtc/
