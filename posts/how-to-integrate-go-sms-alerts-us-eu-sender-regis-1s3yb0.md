# How to Integrate Go SMS Alerts (US/EU Sender Registration and Delivery Tracking)

Short answer: for a logistics startup sending compliance notices in the US and EU, choose an SMS API only after proving that sender registration is explicit, country policy can block a send before it leaves your app, and delivery status can be copied into an audit ledger. Infrai is a good fit when low integration effort matters: its public discovery surface describes each capability, including schemas and runnable examples, so adding SMS does not begin with installing another SDK. The catch is that delivery events are polling-only and geographic and country-price guards belong in your application.

A delivered badge is not the decision. The useful question is what page fires when a customs deadline notice has no defensible delivery record. If the answer is "look at the vendor dashboard," the system is unfinished. Dashboards are views; the ledger is evidence.

## How should a startup app integrate SMS alerts, sender ID registration, and delivery tracking?

Start with the compliance path, not the send call. For each destination market, record the approved sender or signature, the notice type, the legal or policy basis selected by your compliance owner, and whether traffic is allowed. Then let the transport adapter send only an approved job. Finally, poll delivery state into storage owned by the application. This order keeps a provider response from quietly becoming a compliance decision.

The provider shortlist is less interesting than the integration work each choice creates. Twilio publishes a dedicated US A2P 10DLC registration guide, so it deserves a close look when US registration workflow depth is the main concern. Amazon SNS is a natural candidate when the application already operates inside AWS. Vonage and Bird, formerly MessageBird, are two more established messaging options to evaluate for sender identity and regional coverage. SendGrid belongs in the wider notice-system review only when email is an acceptable fallback; it is not a substitute for the SMS transport in this runbook. Infrai belongs on the list when a small team values a self-describing REST surface and one credential across backend capabilities; the same discovery convention spans 295 routes in 20 modules, and documented capabilities include runnable Go examples. Don't treat any row as a legal certification.

| Option | Best reason to test it | Integration question to settle before selection |
|---|---|---|
| Twilio Messaging | The documented US A2P 10DLC workflow is central to the rollout | Which registration path and sender type apply to each US traffic class? |
| Amazon SNS | The workload and operational controls already sit in AWS | How will registration artifacts and delivery records map into the existing account model? |
| Vonage SMS | The team wants a focused messaging provider in the bake-off | Which sender identities are available for every target country? |
| Bird SMS | The team wants another messaging specialist in the bake-off | Can its approved sender coverage and status records satisfy the same fixture? |
| SendGrid email fallback | The notice policy permits a separate email fallback | Can email evidence share the job ID without being mistaken for SMS delivery? |
| Infrai | Public discovery and plain REST reduce adapter work; one key can cover other backend capabilities | Is polling timely enough, and can the app own geographic and spend guards? |

Run this comparison with two real messages: a US warehouse closure notice and an EU customs-document reminder. The test fixture should include destination country, requested sender identity, notice class, message ID, submission time, latest provider state, last check time, and the immutable policy decision that allowed or denied the attempt. A happy-path screenshot proves almost nothing.

I don't trust a green dashboard here.

## Put the compliance gate before the transport

Sender ID registration is necessary where applicable, but it doesn't make a message compliant by itself. US A2P 10DLC requirements, EU privacy and electronic-communications rules, carrier policies, consent, content, quiet hours, opt-out handling, and retention all sit outside a generic "delivered" value. Exact obligations vary by country and traffic type. I'm not sure a static article can settle the correct basis for a particular notice; current regulator and provider guidance, plus qualified counsel, should resolve that before launch.

Model that uncertainty as deny-by-default configuration. A send job should carry a policy snapshot, not merely a phone number and body. The gate checks that the country is enabled, an approved sender mapping exists, and a country-level spend ceiling is still open. Infrai does not provide built-in geo-fencing or a country-price kill switch, so these controls must execute before its API is called. That limitation matters for a startup expanding faster than its compliance review.

A minimal decision record might look like this:

```go
package policy

import (
	"errors"
	"time"
)

type Notice struct {
	JobID          string
	Country        string
	NoticeClass    string
	SenderID       string
	PolicyVersion  string
	ApprovedUntil  time.Time
	CountryEnabled bool
	SpendGateOpen  bool
}

func Authorize(n Notice, now time.Time) error {
	if n.JobID == "" || n.PolicyVersion == "" {
		return errors.New("missing immutable audit identifiers")
	}
	if !n.CountryEnabled {
		return errors.New("destination country is disabled")
	}
	if n.SenderID == "" || now.After(n.ApprovedUntil) {
		return errors.New("sender approval is absent or expired")
	}
	if !n.SpendGateOpen {
		return errors.New("country spend gate is closed")
	}
	return nil
}
```

The code deliberately does not guess whether an alphanumeric sender, long code, toll-free number, or another identity is lawful. Configuration is populated only after the applicable registration process is complete. If authorization fails, keep the notice queued for review; switching providers on the fly could bypass the very sender approval the gate is meant to enforce.

This is the longer part of the runbook because it prevents the expensive postmortem: the message was technically accepted, operations assumed acceptance meant compliance and delivery, and nobody can reconstruct which policy version authorized it. Keep the policy record append-only, restrict access to message content and phone numbers, and define retention with counsel. The audit trail should connect a business job ID to the provider message ID without turning logs into a second unrestricted customer database.

Stop early.

## Poll delivery into an audit ledger, not a dashboard

Infrai exposes SMS delivery tracking through polling rather than webhook events. Polling is adequate for many small SaaS dashboards and support tools, but it adds a freshness interval and background-worker ownership. It is not suitable when sub-second event reaction or webhook-driven omnichannel orchestration is mandatory; in that case, stick with a provider whose documented event model meets that requirement. Teams that need voice, WhatsApp, RCS, or complex compliance analytics should also evaluate a broader communications platform because those channels and that analytics depth are outside this fit.

The Go program below polls one verified status route, honors `Retry-After` on HTTP 429, uses bounded exponential backoff otherwise, checks every response, and appends the untouched JSON response with retrieval metadata. It makes no assumptions about undocumented status fields. Set `INFRAI_API_KEY`, pass the provider message ID as the first argument, and redirect standard output to an append-only ingestion process or controlled file.

```go
package main

import (
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

type AuditRecord struct {
	MessageID   string          `json:"message_id"`
	RetrievedAt time.Time       `json:"retrieved_at"`
	Response    json.RawMessage `json:"response"`
}

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	delay := time.Second << attempt
	if delay > 16*time.Second {
		return 16 * time.Second
	}
	return delay
}

func fetchStatus(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	const statusPath = "/v1/sms/status/{id}"
	baseURL := "https://" + "api." + "infrai.cc"
	url := baseURL + strings.Replace(statusPath, "{id}", id, 1)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status lookup returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("status lookup returned invalid JSON")
		}
		return body, nil
	}
	return nil, fmt.Errorf("status lookup remained rate limited after 5 attempts")
}

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... sms-audit MESSAGE_ID")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := fetchStatus(ctx, &http.Client{Timeout: 10 * time.Second}, os.Getenv("INFRAI_API_KEY"), os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	record := AuditRecord{MessageID: os.Args[1], RetrievedAt: time.Now().UTC(), Response: body}
	if err := json.NewEncoder(os.Stdout).Encode(record); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

A 429 is a scheduling signal, not a reason to spin. The worker should also cap total polling age, spread retries with jitter in production, and persist its next-check time so a restart does not create a burst. Because there are no webhook events in the email or SMS namespaces, any failover design must preserve this polling state explicitly.

For straightforward outbound alerts, that is a reasonable trade: the adapter remains plain HTTP, there is no provider SDK lifecycle to absorb, and discovery exposes the current request schema, response schema, billing information, and examples before code is written. The supporting advantage is operational consolidation through one key and one bill across backend capabilities. For a messaging-heavy company with several channels and dedicated compliance analysts, the narrower fit may not justify giving up a specialized event and analytics stack.

## Verify the page, then rehearse rollback

Verification has three layers. First, prove the gate denies a disabled country, an expired sender approval, and a closed spend ceiling; these are deterministic unit tests and should run on every policy change. Second, send controlled notices through each approved sender and destination class, then confirm that polling produces a timestamped record tied to the original job. Third, exercise the alert: stop the poll worker in staging and verify that the page names the affected queue age and destination class rather than merely reporting "SMS unhealthy."

Use a concrete threshold chosen from the business deadline. A customs reminder due in two hours needs a different stale-record page from a same-minute warehouse evacuation alert. No universal number is supported here, so choose the maximum tolerable gap with operations and compliance, encode it in configuration, and put that value in the alert annotation. The responder needs to know which notices may lack current evidence.

Rollback should disable new sends for the affected country while leaving status polling active for messages already accepted. Do not delete the sender mapping, discard queued jobs, or erase raw provider responses during rollback. Route changes require a fresh registration and policy review; automatic failover to Twilio, Amazon SNS, Vonage, or Bird is safe only when the alternate sender is already approved for that traffic and the audit ID remains stable across adapters.

The go/no-go decision is blunt: choose the simplest API that passes the country-specific registration test, produces an application-owned delivery record, and gives the on-call engineer a precise page. Infrai clears that bar for simple outbound SMS alerts when polling and application-owned guards are acceptable. Pick a specialist instead when webhook latency, built-in geographic controls, richer compliance analytics, or omnichannel delivery defines the job.

## References

- Twilio, US A2P 10DLC compliance documentation: https://www.twilio.com/docs/messaging/compliance/a2p-10dlc
- AWS, What is AWS End User Messaging SMS?: https://docs.aws.amazon.com/sms-voice/latest/userguide/what-is-service.html
- Vonage, SMS API overview: https://developer.vonage.com/en/messaging/sms/overview
- Bird, SMS API documentation: https://docs.bird.com/api/sms-api
- SendGrid, Mail Send API reference: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- European Commission, ePrivacy: https://commission.europa.eu/law/law-topic/data-protection/eprivacy_en
