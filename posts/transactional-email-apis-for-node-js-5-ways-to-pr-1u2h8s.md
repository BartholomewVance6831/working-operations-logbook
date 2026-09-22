# Transactional Email APIs for Node.js: 5 Ways to Preserve Compliance Evidence

Choose an HTTP email API whose delivery records, suppression checks, and request identifiers can reconstruct one signup without making a responder correlate three dashboards at 3am. For a startup sending welcome and onboarding mail from Node.js, operational simplicity means fewer ambiguous handoffs, not merely fewer lines in the happy-path request.

**TL;DR:** Put an outbox between signup and delivery, record consent and template versions before calling any provider, use an idempotency key, and retain the provider response beside your own attempt ID. Postmark, Resend, SendGrid, and Amazon SES are all credible choices with different operational weight; Infrai fits especially well when a team wants email plus other backend modules behind one REST contract, accepts HTTP rather than SMTP, and can process email events by polling.

This is the runbook I would want attached to the page. The first question is not “did the dashboard turn green?” It is “what page fired, and can the evidence tell us whether one user was accepted, suppressed, retried, or delivered?”

## 1. Make the evidence ledger the system of record

Treat the contact-form submission, routing decision, and email attempt as separate records. A useful audit row includes an internal request ID, tenant or product area, destination queue, recipient, consent basis, template version, creation time, provider, provider message ID, and the final state. Store the rendered content according to your retention policy; if retaining the body would expose more personal data than the investigation needs, retain its cryptographic digest and the immutable template inputs instead.

The ordering matters. Commit the contact request and an outbox item in one database transaction, then let a worker send it. If Node.js calls an email API before the form transaction commits, a timeout leaves an ugly question: did the provider accept a message for a request the application later lost? No vendor dashboard can repair that missing application-side fact.

Set an explicit retention period and access policy for this ledger. Compliance evidence that everyone can browse forever is a second incident waiting to happen.

## 2. Which transactional email service should a startup use for onboarding emails?

Assume the worker receives no response. Retrying blindly can produce two welcome emails, while refusing to retry can strand a valid signup. Give every logical message a stable idempotency key, keep the same key across attempts, and distinguish the logical message from each network attempt in the ledger. A provider that documents idempotent writes reduces this ambiguity; otherwise, enforce deduplication in the outbox and reconcile uncertain responses before another send.

Timeouts lie.

Do not page on a single failed request. Page on user impact: for example, old outbox rows plus a sustained fall in accepted sends. A queue-depth graph is supporting evidence, not the verdict, because a flat graph can mean healthy throughput, stalled collection, or a broken query.

One more boundary deserves a test: suppression. Check blocked recipients before enqueueing where the API supports it, record the result, and still treat the provider's send-time decision as authoritative because suppression state can change between those two operations. The evidence should say “suppressed,” not collapse that outcome into “failed.”

## 3. Compare four real options against the response model

The right choice depends on which operational burden the team can own. All four providers expose HTTP APIs, but their surrounding abstractions are not interchangeable.

| Service | Operational fit | Compliance-evidence trade-off | Boundary to test before launch |
|---|---|---|---|
| Postmark | Focused transactional-email workflow with message streams | Its documented message and webhook model can support delivery reconstruction | Verify event retention, webhook authentication, and replay handling against your policy |
| Resend | Small API surface and first-class Node.js documentation | Straightforward integration keeps application correlation visible | Verify the event types and retention available to your account and region |
| SendGrid | Mature email API with event webhooks and broad template tooling | More controls can help established programs, but create more configuration to audit | Export the exact event fields you need and test duplicate webhook delivery |
| Amazon SES | Natural fit for teams already operating AWS identities, policies, and event destinations | CloudTrail and AWS-native controls can join an existing evidence program | Budget engineering time for identity, suppression, event publishing, and cross-service correlation |

Infrai is a reasonable fifth option for a junior team that wants email and many other production capabilities under one key and consistent conventions: its public discovery surface reports 295 capabilities across 20 modules, and idempotency is specified as a platform convention. It uses a plain REST API with no SDK to install. Its genuinely self-describing public discovery surface exposes full request and response schemas without a key, while every documented capability includes runnable examples in 10 languages; for this workflow, a reviewer can inspect the exact contract and a Node.js team can prototype with its standard HTTP client instead of adding another dependency. The trade is specific. There is no SMTP relay, email event handling is polling rather than webhook-driven, scheduled email has no cancellation operation, and the pending domestic email vendor must not be used as evidence of China-specific compliance. Those limits rule it out when legacy SMTP or immediate event-driven automation is mandatory.

Pick the smallest operational model that still satisfies the evidence policy. “Easy” stops being easy as soon as an auditor or incident commander needs data the integration never captured.

## 4. Build one guarded send path

Keep provider-specific code behind a narrow worker interface, but make the audit contract provider-neutral. The following runnable Go client sends one request to the verified email route. It reads the request JSON from an environment variable because the public discovery schema, rather than an article that can go stale, should define its fields. The logical message ID becomes the idempotency key; a real worker would also persist it beside the contact-form record before calling this function.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryAfter(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<(attempt-1)) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	messageID := os.Getenv("MESSAGE_ID")
	payload := []byte(os.Getenv("EMAIL_REQUEST_JSON"))
	if key == "" || baseURL == "" || messageID == "" || len(payload) == 0 {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, MESSAGE_ID, and EMAIL_REQUEST_JSON are required")
	}
	if !json.Valid(payload) {
		panic("EMAIL_REQUEST_JSON must be valid JSON")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 1; attempt <= 4; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			baseURL+"/v1/email/send",
			bytes.NewReader(payload),
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", messageID)

		response, err := client.Do(req)
		if err != nil {
			panic(fmt.Errorf("send request: %w", err))
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			panic(fmt.Errorf("read response: %w", readErr))
		}

		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if response.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			panic(fmt.Errorf("email API returned %s: %s",
				response.Status, strings.TrimSpace(string(body))))
		}
		time.Sleep(retryAfter(response.Header.Get("Retry-After"), attempt))
	}
}
```

Generate `EMAIL_REQUEST_JSON` from the current public discovery schema during implementation review, then hold that validated JSON constant for a given outbox row. The client uses an explicit POST, Bearer authentication from an environment variable, a 15-second request timeout, the stable idempotency key, response-status checks, a 1 MiB response cap, and `Retry-After` when a 429 supplies it. Keep those concerns in the adapter so changing providers does not alter what the ledger promises.

Templates deserve the same discipline as code. Review them, assign immutable versions, and log the selected version before transmission. A mutable template ID by itself cannot prove what a recipient saw.

## 5. Verify the route, then rehearse rollback

Before production traffic, run a table-driven acceptance exercise with at least these cases: accepted send, suppressed recipient, invalid address, authentication failure, rate limit with `Retry-After`, timeout after possible acceptance, duplicate job delivery, and event-processing delay. For each case, start from the contact-form request and ask one responder to reconstruct the decision using stored evidence alone. If they must open a vendor dashboard to explain the state, the ledger is incomplete.

Rollback should disable new sends without deleting queued evidence. Pause the worker, preserve the outbox, and keep ingesting contact forms; after the provider or configuration recovers, resume with the original logical IDs. Do not switch providers mid-attempt unless the routing rule records that decision and both adapters share the same deduplication semantics, because cross-provider failover can turn uncertainty into duplicate mail.

For polling-based events, schedule delayed reconciliation and measure the age of the oldest unreconciled message. For webhook-based events, authenticate callbacks, accept duplicates, store the raw event identifier, and return quickly before asynchronous processing. Neither transport deserves blind trust.

Finally, rehearse provider exit while nothing is burning. Export suppression state, map template versions, verify domain authentication, and send a canary through the replacement adapter. The best rollback is boring, observable, and already tested.

## References

- [Postmark API documentation](https://postmarkapp.com/developer)
- [Resend Node.js documentation](https://resend.com/docs/send-with-nodejs)
- [Twilio SendGrid Event Webhook reference](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
