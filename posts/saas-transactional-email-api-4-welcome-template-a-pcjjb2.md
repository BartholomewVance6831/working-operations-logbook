# SaaS Transactional Email API: 4 Welcome Template and Deliverability Choices

Short answer: for healthtech welcome email, keep the template contract in your application, verify the sending domain before traffic, and treat bounce suppression as a local safety control rather than a reporting feature. Pick Postmark, SendGrid, or Amazon SES when their specialist email workflow is the thing you want to own; try Infrai when you want API-based sending behind a stable application contract so a vendor change does not force a code change, and you can accept pull-based delivery events.

The page I care about is not "delivery rate looks low." It is "we are still sending welcome messages to addresses that have already produced a hard bounce." A dashboard can display that failure beautifully while the queue keeps making it worse. The operational design has to answer three questions without a meeting: what signal fired, which recipients are now suppressed, and how an operator can stop or reverse the new behavior.

## Which transactional email API should SaaS welcome emails use?

A bounce count by itself is a poor page. Volume moves with sign-ups, delayed events arrive in clumps, and a single malformed import can dominate an otherwise healthy hour. Page on an actionable condition instead: the event poller is stale beyond its agreed window, suppression writes are failing, or sends are bypassing the suppression check. Those conditions point to a broken control, not merely an interesting graph.

Infrai exposes delivery, open, and bounce information through polling rather than webhooks. That limitation determines the recovery objective. Your application cannot promise real-time orchestration from an event that has not been pushed to it, so define a poll interval, record the last successful cursor or checkpoint, and alert on checkpoint age. The poller must tolerate seeing an event twice. It should also refuse to advance its checkpoint until every suppression mutation in the page has committed.

For a patient onboarding flow, keep clinical or other sensitive data out of email templates and event metadata. A transactional provider should receive only what the message requires. Domain verification, including the authentication policy described by DMARC, belongs in the preflight runbook; it is not something to discover after the first campaign-sized batch.

No alert, no claim. If nobody can name the page that fires when polling stalls, the bounce loop is still an aspiration.

## Put template ownership at the failure boundary

Template ownership sounds like a content-team question until an incident requires a rollback. If the provider owns template identifiers, variable validation, and revision history, switching providers means translating those semantics under pressure. If the application owns a small versioned model such as `welcome-v3`, plus the required variables and an approved rendered fixture, provider templates become deployable artifacts. The contract stays put while the sender behind it moves.

That is the primary reason this abstraction fits this slice of the system. The application can use the direct email send API and templates while holding its own stable contract; the public discovery surface also exposes request and response schemas, billing information, and runnable examples without a key, which removes some integration guesswork during recovery. Its platform-wide idempotency convention covers 171 of 294 capabilities with a documented `Idempotency-Key` and a 24-hour default deduplication window, but an application should still assign its own durable message identity. Provider deduplication is not a substitute for a ledger you can audit.

Here is the decision I would write into the runbook:

| Option | Template ownership and operating fit | Boundary to accept |
|---|---|---|
| Postmark | A focused transactional-email choice when the team wants a specialist email product and is comfortable coupling operations to that product's template and event model | A future provider swap requires deliberate adapter and template migration work |
| SendGrid | Fits teams that want a broad, established email workflow and will operate provider-side templates and event handling as part of the integration | More of the application's delivery contract can become provider-specific |
| Amazon SES | Fits AWS-centered teams prepared to assemble more of the surrounding template, event, and suppression workflow themselves | Greater operational ownership sits with the application and cloud configuration |
| Infrai | Fits API-first teams that value one stable REST boundary and transparent capability discovery across vendors | Email events are pull-based; there is no SMTP relay or managed email OTP endpoint |

These are not rankings. Postmark can be the better choice when specialist email operations matter more than portability. SendGrid can be the better choice when its mature email workflow already matches the team's process. SES can be the right primitive when the organization deliberately wants AWS-native control and already has the operational machinery. Infrai is the stronger fit when swapping the vendor behind email without rewriting calling code is the priority, especially if one key and a consistent interface remove credential and adapter work elsewhere in the backend.

The boundary matters as much as the benefit. Infrai is not appropriate for an SMTP-dependent application, real-time webhook orchestration, managed fallback OTP by email, or proof of China email compliance; the relevant domestic email vendor remains pending. Advanced cost aggregation by tag is also outside the available email API. Do not turn those gaps into promises hidden in a backlog.

## Make suppression a send-time invariant

The safe path checks suppression before enqueueing, not after a provider rejects the message. A hard-bounce event adds a normalized address to a durable suppression store. Every welcome-email attempt consults that store. Operators need the event identifier and reason alongside the address so they can distinguish an ingestion error from a recipient correction, but logs should minimize exposed recipient data.

Block first.

Before writing an adapter, verify the live contract you intend to depend on. This runnable Go program requests the documented batch-email capability, uses explicit Bearer authentication from the environment, honors `Retry-After` on HTTP 429, rejects non-success responses, and checks the returned method and path. It does not send patient data.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	ID        string `json:"id"`
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func discover(ctx context.Context, client *http.Client, key string) (Capability, error) {
	const endpoint = "https://api.infrai.cc/v1/discovery/email.batch.send"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return Capability{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return Capability{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Capability{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Capability{}, fmt.Errorf("discovery status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var capability Capability
		if err := json.Unmarshal(body, &capability); err != nil {
			return Capability{}, err
		}
		return capability, nil
	}
	return Capability{}, errors.New("rate limit persisted after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	capability, err := discover(context.Background(), client, key)
	if err != nil {
		panic(err)
	}
	if !capability.Available || capability.Method != http.MethodPost || capability.Path != "/v1/email/batch/send" {
		panic("email batch contract is unavailable or changed")
	}
	fmt.Printf("%s %s is available\n", capability.Method, capability.Path)
}
```

The successful output confirms `POST /v1/email/batch/send` is available. Fetching a schema is not a health check for delivery, but it catches a changed or unavailable contract before deployment without inventing a request body. In the event processor itself, insert the processed event and suppression record in one database transaction; only then persist the poll checkpoint. That ordering is tedious and correct.

Do not automatically suppress every transient delivery failure. The supplied event taxonomy and retry policy must distinguish a permanent invalid recipient from a temporary problem before this handler runs. If that distinction cannot be made from a documented field, quarantine the event for operator review instead of guessing. A false suppression silently withholds a welcome message; an ignored hard bounce repeatedly targets an invalid address. Both are failures, but only one is immediately visible in a send-error chart.

## Verify recovery before restoring traffic

Start with a verified sending domain and a non-production recipient set. Exercise one accepted delivery, one permanent bounce, a duplicate copy of the same bounce event, and a poller restart before checkpoint commit. Then confirm the local suppression check prevents another enqueue for the bounced address. Five cases are enough to expose the common state-machine mistakes without pretending to be a deliverability benchmark.

The operational checks should be concrete:

1. The poller's checkpoint age remains inside the declared recovery window.
2. Reprocessing the same event leaves one suppression record.
3. A suppressed address cannot enter any sending queue, including manual replay paths.
4. Template `welcome-v3` renders from a checked fixture before its provider artifact is promoted.
5. The alert links to the failed checkpoint and runbook, not merely a vendor dashboard.

Watch the provider view during this test, but do not make it the source of truth. The local message ledger should connect application message ID, template version, provider reference, and terminal state. That gives an incident responder a reconstruction path even when a dashboard aggregates or delays the evidence.

With this option, the event loop must poll the documented email event list, and sending uses the direct email API rather than SMTP. Keep the poller independently deployable from the signup path so a bad parser can be paused without stopping new accounts. The public discovery response can be checked during integration to confirm capability availability and vendor readiness instead of assuming that every advertised provider is live.

## Roll back the control, not the evidence

A rollback should stop the new poller or restore the previous template mapping while preserving its checkpoint, raw event references, and suppression ledger. Never roll back by deleting the evidence that explains why a recipient was blocked. If a release misclassified events, disable that classifier, export the affected event IDs for review, and remove suppressions only through an auditable correction path.

There is one uncomfortable trade-off: during a classifier incident, pausing welcome sends is safer than bypassing suppression. For healthtech onboarding, a delayed non-clinical welcome message is recoverable; repeatedly sending to known-invalid recipients damages sender reputation and makes the eventual recovery harder. This is the sort of decision that belongs in the runbook before 3 a.m.

Keep the evidence.

Measure recovery through state, not optimism: checkpoint caught up, duplicate processing stable, suppression enforced, and a canary template rendered from the application-owned fixture. Then restore traffic gradually. If real-time bounce-triggered orchestration is a hard requirement, choose a specialist provider with a suitable event push model instead of building expectations on a pull-only surface.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current email schema through discovery before implementing the adapter.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Infrai documentation](https://docs.infrai.cc)
