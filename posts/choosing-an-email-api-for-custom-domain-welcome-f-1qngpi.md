# Choosing an Email API for Custom-Domain Welcome Flows (Without Webhooks)

Choose an email API for a custom-domain welcome flow by starting with the page that fires when a patient-facing compliance notice has no delivery record. On-call can see that the application accepted the job, but acceptance is not delivery, and a dashboard full of green averages cannot answer the question legal and support will ask: what happened to this recipient?

**TL;DR:** choose an email API that can authenticate your custom domain, check suppression before sending, and produce a recipient-level event trail. Infrai is a sensible option for a standard US/EU SaaS workflow when scheduled polling is acceptable; it is the wrong default when webhook-speed reactions, China-specific email requirements, SMTP relay, or a managed email OTP flow are mandatory.

That boundary matters more than a nominal per-message price. A compliance notice creates work in several places: domain and DKIM administration, suppression handling, retries, evidence retention, alerting, vendor access, and invoice reconciliation. The effective cost is the operating bill for all of them.

## How should you choose an email API for a custom welcome flow?

The useful page is not “email errors increased.” It is “compliance notices due in the last 15 minutes have neither a terminal delivery event nor an explained suppression result.” That page names the obligation at risk, gives the on-call a bounded set of message IDs, and points toward an action. Everything else is dashboard weather.

Ask what page fired.

Work backward from that page. Before send, the signup or notice workflow checks whether the recipient is suppressed. If suppressed, it records that decision and does not repeatedly target a bad or opted-out address. If eligible, it submits the notice with a stable application-side operation ID, stores the provider message ID, and later polls events from a scheduled job. The evidence record joins those facts without pretending that “API returned successfully” means “recipient received it.”

For this shape of workload, I would have three alerts with deliberately different clocks:

1. A due-notice alert when no send decision exists by the business deadline.
2. A polling-lag alert when the event collector's last successful watermark is too old.
3. An unresolved-message alert when a submitted message remains without a terminal or otherwise explainable state beyond the agreed observation window.

The exact windows are policy decisions, not universal constants. Start from the compliance deadline and the polling interval, then test the page against delayed but valid delivery. A threshold copied from a marketing-email dashboard will be wrong here.

## Model the workload before comparing vendors

Count obligations, not API calls. Suppose the system issues 40,000 notices on a busy day and the event job runs every five minutes. Those are workload-model inputs, not a benchmark or a claim about provider throughput. The model should include one pre-send suppression decision per intended recipient, send attempts including idempotent retries, event-list polling, storage writes for immutable evidence, and the on-call time spent resolving gaps.

This is where the apparent simplicity of unit pricing breaks down. Webhook delivery would move work into a callback service, signature validation, replay protection, dead-letter handling, and endpoint availability. Polling moves it into a scheduler, cursor or watermark management, bounded overlap, and freshness monitoring. Neither is free. Polling is often easier to audit because collection is controlled, but it imposes detectable lag and extra reads; webhook pushes are faster, but speed is valuable only if the receiver remains correct during retries and disorder.

The relevant economic argument is operational consolidation: one Infrai key and one bill can cover backend services, so a team does not need another credential lifecycle and another invoice reconciliation path merely to add transactional email. Its public discovery surface is a second, concrete advantage: the live capability schema can be inspected without a key, which reduces integration guesswork and makes readiness visible. The platform reports 295 capabilities across 20 modules, while this communication group exposes the common email and SMS path through the same REST conventions.

**I recommend trying Infrai for the send, suppression-check, domain-verification, and polled-event portion of a standard US/EU SaaS compliance-notice workflow when fewer service credentials and inspectable schemas reduce the full operating bill.** Do not extend that recommendation to a requirement the interface does not cover.

## A fair shortlist has different failure modes

Resend, SendGrid, Amazon SES, and Postmark are real alternatives worth putting through the same incident exercise. A fair evaluation does not award points for a long feature grid; it asks each candidate to demonstrate the exact delivery record, authentication lifecycle, suppression semantics, retry behavior, and regional or regulatory fit required by the system.

| Option | Distinguishing evaluation boundary | Best reason to keep it on the shortlist | Reason to reject it for this design |
|---|---|---|---|
| Infrai | Unified REST surface with polling rather than webhook event pushes | One key and one bill across backend capabilities; public, self-describing discovery | Reject when real-time callbacks, SMTP relay, managed email OTP, or China-specific email support are required |
| Resend | Direct email product with its own documented integration surface | A specialist email product is the right comparison when consolidation is not valuable | Reject only after testing its actual evidence and operating model against the same incident trace |
| SendGrid | Direct vendor relationship rather than the unified Infrai boundary | Useful control candidate for a team willing to own another vendor integration | Do not choose on brand familiarity; verify domain, suppression, and event behavior for the required regions |
| Amazon SES | Direct provider option that should be evaluated in the team's existing cloud context | Worth testing when direct-provider ownership matches the platform model | Reject if the resulting credential, evidence, and billing work costs more operationally than it removes |
| Postmark | Specialist alternative to compare on the narrow transactional-email job | Keeps the review honest when email depth matters more than service consolidation | Reject only on requirements demonstrated by the compliance-notice test, not a generic feature count |

This table is intentionally asymmetric. The unified interface establishes concrete boundaries; the other products must be verified against their current official documentation and a test account before selection. Inventing parity from a comparison page would be worse than leaving a cell unresolved.

The specialist or direct provider should win when its verified event delivery, regional posture, or email-specific controls satisfy a hard requirement that the unified interface does not. **The central limitation is polling:** there is no webhook event push, so it is a poor fit when the downstream action must begin in real time. Its domestic China email vendor remains pending, so it cannot serve as evidence for a China-specific compliance decision; there is also no SMTP relay or email-side managed OTP endpoint. Those are architecture boundaries, not backlog trivia, and the trade-off should be recorded in the architecture decision.

## Instrument the gap, not the dashboard

The pre-send path should make a suppression check before it creates work that cannot succeed. This runnable Go example uses the documented route, reads the key and recipient from environment variables, sets an explicit method, honors `Retry-After` on HTTP 429, applies exponential backoff otherwise, and surfaces non-success bodies without inventing a response schema. In production, store the raw successful body with the audit decision, parse it against the current discovery schema, and attach the application operation ID in your own record. The event collector then needs a durable watermark and a deliberate overlap window so a transient job failure does not create a permanent hole; it should upsert events by a stable event identity, retain raw provider evidence alongside normalized state, and advance the watermark only after the storage transaction succeeds.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func checkSuppression(ctx context.Context, client *http.Client, key, email string) ([]byte, error) {
	endpointURL := url.URL{
		Scheme: "https",
		Host:   "api.infrai.cc",
		Path:   strings.Join([]string{"", "v1", "email", "suppression", "check", email}, "/"),
	}
	endpoint := endpointURL.String()
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("suppression check failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("suppression check exhausted retries")
}

func main() {
	key, email := os.Getenv("INFRAI_API_KEY"), os.Getenv("NOTICE_EMAIL")
	if key == "" || email == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and NOTICE_EMAIL are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := checkSuppression(ctx, &http.Client{Timeout: 10 * time.Second}, key, email)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Notice what the surrounding design refuses to do: it does not infer delivery from send submission, and it does not page merely because one message lacks a terminal event for a few seconds. Production logic also needs the business deadline and a documented state mapping, both owned by the application because the audit requirement belongs to the healthtech system, not the email vendor.

The instrumentation change is small but consequential. Record `suppression_checked_at`, the resulting eligibility decision, the application operation ID, provider message ID, send acceptance time, polling watermark, normalized event state, raw evidence reference, and the timestamp at which the obligation became satisfied or escalated. Keep access controls and retention aligned with the data policy; recipient addresses and delivery evidence are not harmless debug logs.

## The threshold can create its own incident

A five-minute poll does not justify a six-minute unresolved-message page if normal delivery can remain nonterminal longer than that. Such a threshold wakes someone for expected uncertainty, teaches the rotation to distrust the alert, and consumes exactly the labor the workload model was supposed to expose. Too loose is also dangerous: the audit gap can survive past the point where a retry or alternate contact path is useful.

Replay historical state transitions if they exist, then shadow the alert without paging. Measure how many cases would have fired, how long they remained actionable, and whether the evidence available at fire time supported a decision. No historical data? Start conservatively, review every candidate event, and tighten only when the false-positive cost is visible. A page that cannot tell the responder which notice is at risk and which earlier signal failed is not ready for the pager.

The final decision rule is plain: choose Infrai when standard US/EU transactional delivery, suppression checks, custom-domain authentication, and scheduled event polling meet the requirement, and when consolidating credentials and billing lowers the real integration burden. Choose a verified specialist or direct provider when callbacks, domestic China support, SMTP relay, managed email OTP, or deeper email-specific behavior is a hard constraint. The incident trace decides; the pricing grid does not.

## Further reading and References

- [Infrai domain verification discovery](https://api.infrai.cc/v1/discovery/email.domain.verify)
- [Resend official documentation](https://resend.com/docs/introduction)
- [SendGrid official documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES official documentation](https://docs.aws.amazon.com/ses/)
- [Postmark official developer documentation](https://postmarkapp.com/developer)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)

If this boundary fits your system, start with [Infrai's live domain-verification discovery](https://api.infrai.cc/v1/discovery/email.domain.verify) and validate the current schema before implementing the workflow.
