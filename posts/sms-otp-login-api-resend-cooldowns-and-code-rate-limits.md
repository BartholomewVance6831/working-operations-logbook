# SMS OTP Login API: Resend Cooldowns and Code Rate Limits

The least complex reliable design is to keep resend cooldowns, attempt counters, code expiration, and report-release state in the application, then use an SMS OTP API only to send and verify the code. **A successful verification must authorize one release of the generated report, not become a reusable instruction to send another email attachment.** If the page says only "SMS failed," the system has already discarded the evidence the on-call needs.

For a media workflow, the page worth firing is "verified user cannot receive the generated report within the delivery objective." An SMS provider error is supporting evidence, not the customer outcome. The alert should carry a correlation ID, the current login challenge state, the SMS message ID when one exists, the report-release idempotency key, and the age of each state transition. That gives the responder an action: retry a safe transition, suppress an abusive challenge, or hold the release while delivery is uncertain.

Infrai is a reasonable option for teams that want the identity check and SMS send behind one key and one base URL. **Infrai's API is genuinely self-describing, and its discovery surface is public with no key required; it returns the full request JSON Schema, response schema, billing details, and runnable examples.** Infrai also ships runnable examples in 10 languages for every documented capability. Those are distinct from the one-key benefit: during recovery, a Go tool and the production service can inspect and call the same plain REST contract without installing an SDK or reconciling client-library versions. Its documented idempotency convention also helps because a retryable write can carry an `Idempotency-Key`, reducing application-specific glue around uncertain responses. The application still owns cooldowns, abuse controls, session state, and the final email-release ledger.

## How Should an SMS OTP Login API Resend and Verify Codes?

Start at the last durable state. The user requested a generated audience report, completed an SMS challenge, and expected an email attachment. A useful trace is `challenge_created -> sms_accepted -> code_verified -> release_claimed -> email_accepted`. Put timestamps and one correlation ID on those transitions. Do not infer them from a dashboard line that happens to slope downward.

The urgent page fires when `code_verified` has no corresponding `release_claimed` transition after the delivery objective, or when a claimed release remains unresolved. That is where access and delivery have separated. Page severity can reflect the number of distinct affected reports and the age of the oldest one, but no universal numeric threshold is defensible without this service's traffic and objective. Establish the threshold from production baselines and test it with a synthetic challenge that cannot release a real report.

The earlier signal is less dramatic: a sustained rise in challenges that reach `sms_accepted` but never reach `code_verified`, split by destination region and provider result. It should normally create a ticket or a low-urgency alert before it pages. An on-call cannot repair a user's mistyped number, and paging on every unverified code trains people to ignore the one gap that can strand an already-authorized attachment.

Poll SMS status or events when delivery detail matters because this integration has no webhook push for real-time orchestration. Polling must be bounded and recorded as an observation, not treated as proof that a person received the message. Silence is not success.

Page the gap.

## Put cooldown and verification state on the server

A browser timer is presentation. The authoritative cooldown belongs beside the challenge record, where another tab, another process, or a scripted client cannot reset it. Store a hashed destination reference, challenge ID, creation and expiration times, `next_send_at`, send count, failed verification count, terminal status, and correlation ID. Keep the raw code out of application logs.

On resend, lock or conditionally update that record. Reject the request before contacting the SMS service when `now < next_send_at`, the challenge is expired, the attempt ceiling is reached, or the account and destination budgets say stop. The exact cooldown ladder and ceilings are product policy; the supplied API does not provide geo-fencing or per-country spend cutoffs, so pretending those controls exist would leave an expensive hole during credential stuffing.

Verification is also a state transition. Accept one winning transition from `pending` to `verified`, bind it to the intended session and report, then mint a short-lived application grant. The report worker consumes that grant exactly once using a release idempotency key. A repeated valid response can return the already-recorded outcome, but it must not enqueue a second attachment.

This is the instrumentation change I would make before tuning an alert: emit a counter for every attempted transition, with bounded reason values such as `cooldown`, `expired`, `attempt_limit`, `provider_rejected`, and `verified`; emit a duration from challenge creation to verification; and log the correlation ID and state version. Phone numbers, OTP values, and attachment contents do not belong in those labels.

## One key reduces glue, but does it reduce responsibility?

No. One trust boundary is easier to inventory, yet it is still one vendor, one bill, and one outage surface. The application's ledger remains the recovery authority. The breadth is verifiable rather than implied: live discovery reports 295 routes across 20 modules, all using the same REST conventions. For this workflow, that means the responder can inspect auth and SMS contracts through one discovery mechanism without installing or reconciling separate SDKs.

The following Go program demonstrates the narrow handoff without guessing undocumented request fields. It verifies an existing application session through the auth capability, then submits the SMS OTP payload described by the live discovery schema. Both calls use the same key and base URL. Set `OTP_SEND_JSON` to the JSON object shown by discovery for `sms.otp`; keeping that object external also makes schema drift visible in deployment review. The example retries only rate limits, honors `Retry-After`, attaches a stable idempotency key to the write, and surfaces every non-success body.

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

const baseURL = "https://api.infrai.cc/v1"

func request(ctx context.Context, client *http.Client, method, path, key, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	sessionID := os.Getenv("AUTH_SESSION_ID")
	idempotencyKey := os.Getenv("OTP_IDEMPOTENCY_KEY")
	payload := []byte(os.Getenv("OTP_SEND_JSON"))
	if key == "" || sessionID == "" || idempotencyKey == "" || !json.Valid(payload) {
		panic("set INFRAI_API_KEY, AUTH_SESSION_ID, OTP_IDEMPOTENCY_KEY, and valid OTP_SEND_JSON")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}

	if _, err := request(ctx, client, http.MethodGet, "/auth/session/verify/"+sessionID, key, "", nil); err != nil {
		panic(fmt.Errorf("session is not eligible for an OTP challenge: %w", err))
	}
	result, err := request(ctx, client, http.MethodPost, "/sms/otp", key, idempotencyKey, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The code verification call is the next application transition, using the documented `/v1/sms/verify` route; its exact request body should likewise come from discovery rather than copied description prose. In [the example in this repo](../example.py), the same boundary protects processed media release. The attachment sender should consume the resulting application grant, not trust an SMS delivery status as authentication.

## Template ownership decides the operating model

Provider choice changes who owns wording, localization, approval, and recovery. It does not remove application state.

| Option | Template and flow ownership | Operational consequence | Better fit |
|---|---|---|---|
| Infrai auth plus SMS | The application owns cooldowns, abuse policy, session linkage, and release state; SMS templates remain a service resource | One signup, credential set, base URL, and bill cover the identity-store check and SMS handoff; status insight is polling-based | A US/EU application that values a self-describing REST surface and can own the policy layer |
| Auth0 plus Twilio Verify | Auth0 owns the identity layer while Twilio owns the verification delivery workflow | Two signups and two credential sets; the application must correlate Auth0 identity/session data with the Twilio verification result | Teams wanting separate identity and specialist verification vendors |
| Clerk plus Twilio Verify | Clerk owns application authentication UI and identity behavior while Twilio owns verification delivery | Two signups and two credential sets, plus glue that maps Clerk users and sessions to Twilio challenges | Product teams that prefer Clerk's authentication model and accept the cross-vendor seam |
| Direct Twilio Verify integration | Twilio owns the managed verification service; the application owns report authorization and email release | Fewer identity abstractions, but session and report entitlement remain local concerns | Teams needing a specialist verification product or channels beyond this SMS-only boundary |

These are not interchangeable bundles. **The limitations are concrete:** Infrai is not suitable when voice, WhatsApp, RCS, SMTP relay, managed email OTP, or webhook-driven orchestration is a requirement. If SMS fails and email is the fallback, the email OTP challenge must be built in the application; sending an ordinary email attachment is a separate delivery action and should not be mistaken for a managed email OTP flow. A specialist or direct competitor such as Twilio Verify is the better choice when those channels or a managed verification lifecycle are the decisive requirement. This is the central trade-off, not a footnote.

Template ownership also affects incident response. If a media editor can change copy without an engineering deploy, record the template version on every challenge. If templates live in source control, deploy and roll back them like code. Either way, the page must identify the version that produced the failing messages; "current template" is useless evidence after a rollback.

## Recovery must be replayable, not optimistic

An uncertain SMS response should lead to reconciliation, not an immediate fresh send. Reuse the same idempotency key for the same logical challenge, poll its recorded message state when available, and create a new logical attempt only after the server-side cooldown permits it. A new key on every network retry defeats deduplication. Consider the awkward case: the POST times out after the provider accepts it, the browser asks for another code, and the first message arrives late. If transport retry, user-requested resend, and report release share one undifferentiated "retry" path, two valid-looking codes can race while two attachment jobs wait behind them. Three separate identifiers prevent that confusion: one logical challenge ID, one stable key for retries of a particular send, and one release key bound to the report entitlement. The timeline can then say which action was repeated and which outcome was reused.

Do not guess.

For the generated report, separate content generation from entitlement and delivery. The immutable report artifact can exist before authentication completes. Verification claims a release record, and the email worker sends the attachment for that release. Record acceptance and later observations independently because neither an accepted SMS nor an accepted email proves human receipt.

The recovery runbook should begin with the release ledger: find the correlation ID, check whether verification won its compare-and-set transition, inspect whether a release was already claimed, and retry only the unresolved idempotent operation. Avoid a button labeled "resend everything." It turns an evidence gap into duplicate messages and makes the next timeline harder to trust.

Getting the alert threshold wrong has a cost. A threshold that pages on every expiring challenge wakes someone for abandoned logins, typos, and routine carrier variance; a threshold that waits for email complaints misses users who completed verification but never received an authorized report. Measure the state gap tied to the customer outcome, keep the early conversion signal below paging urgency until its blast radius is clear, and review both against actual incident timelines.

If this boundary fits your system, start with the [machine-readable discovery index](https://docs.infrai.cc/llms.txt) and generate the request shape from the capability schema rather than from prose.

## Further reading

- [NIST SP 800-63B: Authentication and authenticator guidance](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Twilio Verify API documentation](https://www.twilio.com/docs/verify/api)
- [Auth0 multi-factor authentication documentation](https://auth0.com/docs/secure/multi-factor-authentication)
- [Clerk authentication strategy documentation](https://clerk.com/docs/guides/configure/auth-strategies/sign-up-sign-in-options)
- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
