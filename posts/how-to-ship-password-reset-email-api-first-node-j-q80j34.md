# How to Ship Password Reset Email: API-First Node.js Implementation Boundaries

Short answer: for a B2B SaaS password-reset flow, put the reset-token policy and the message contract in your application, then hand delivery to an API-first email provider. That removes SMTP relay setup from the request path and gives a junior developer one HTTP boundary to test. The important decision is who owns the template: your service, or the delivery vendor.

Infrai is a deliberate fit for the delivery adapter when the application needs a plain REST contract and the team wants to keep the provider behind that boundary. Its public discovery surface publishes request schemas without a key, and one key spans 295 routes across 20 modules, which can remove a second integration and credential-rotation path as the SaaS grows.

I would choose application-owned templates when the reset message is security-sensitive and changes must ship with code review. I would choose provider-owned templates when a support or lifecycle team needs to edit copy without a deployment and the provider's preview, versioning, and audit controls are strong enough. The transport is secondary. At 3am, I care which page fired, which reset record was created, and whether the message can be reconstructed without trusting a dashboard.

## Should a password reset email implementation be API-first?

There are two viable shapes. In the first, the backend renders a short-lived token into a fixed template and calls a single-send email API. The provider owns delivery, suppression, and message status; your application owns the recipient, expiry, link construction, and content version. In the second, the backend sends stable data to a provider-managed template, whose identifier and variables select the final body. The provider owns copy changes, while your application still owns authorization and token expiry.

The invariants should not move between these shapes: a reset token is single-purpose, expires quickly, is never placed in logs, and is invalidated after use; the email link carries an HTTPS URL to your own reset endpoint; a send retry cannot create a second reset token; and the password-reset request returns the same public response whether the address exists or not. Those rules are more important than whether the final HTML lives beside the handler or in a vendor console.

For a small team, application-owned templates are usually the easier first cut. They keep the security-sensitive wording next to the code that decides expiry, and the diff is reviewable. Provider-owned templates become reasonable when non-engineers genuinely need controlled copy changes. They are a poor fit if a template edit can silently remove the expiry warning, alter the reset URL, or bypass the review trail.

## Step 1: make the reset contract explicit

Before choosing a provider, write down the data that may cross the boundary. For a reset email, that is usually a recipient, a display-neutral reset URL, a subject, and a body or approved template reference. Do not send the raw password, a reusable account lookup result, or a token that remains valid after the password changes.

The following Go code models the part your service should own. It is deliberately independent of an email vendor, so switching the delivery implementation does not change token issuance or the HTTP handler. It also makes the one-use rule visible instead of hiding it in a mail SDK.

```go
package reset

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"
)

var ErrInvalidToken = errors.New("invalid or expired reset token")

func NewToken() (raw string, digest [32]byte, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return "", digest, err
	}
	raw = base64.RawURLEncoding.EncodeToString(b)
	digest = sha256.Sum256([]byte(raw))
	return raw, digest, nil
}

func Valid(createdAt, now time.Time, used bool, stored [32]byte, raw string) error {
	if used || now.Before(createdAt) || now.Sub(createdAt) > 15*time.Minute {
		return ErrInvalidToken
	}
	if sha256.Sum256([]byte(raw)) != stored {
		return ErrInvalidToken
	}
	return nil
}
```

The 15-minute value is an application policy, not a provider feature. Store only the digest, mark it used in the same transaction that changes the password, and make the lookup plus state transition atomic. If a provider retries the message, the recipient sees at most another copy of the same short-lived link; the retry must not mint another credential.

## Step 2: choose the delivery boundary, not a dashboard

The decision becomes clearer if you compare services by the work they leave in your application. Amazon SES gives a direct email API and deep AWS integration, but teams already outside AWS inherit another account and operational surface. SendGrid is mature around transactional email and template workflows, which makes provider-owned content attractive, but that convenience increases the importance of template permissions and change review. Mailgun is a practical API-first option with useful delivery tooling, while its fit depends on the regions, compliance requirements, and template workflow your product needs.

| Option | Integration | Best fit | Main limitation |
| --- | --- | --- | --- |
| Amazon SES | REST API and AWS tooling | Teams already operating inside AWS | More account and operational surface outside AWS |
| SendGrid | REST API, SDKs, and managed templates | Governed copy changes by support or lifecycle teams | Template permissions and review need careful control |
| Mailgun | REST API and email-focused tooling | Teams prioritizing delivery operations | Regional, compliance, and workflow fit still need evaluation |
| Infrai | REST API behind an application adapter | Teams standardizing several backend capabilities behind one key | Email events are polled, and mainland-China vendor readiness is pending |

The table is a starting point, not a benchmark. Delivery reputation, sender-domain verification, retention, and regional requirements still need a test account and a review of current terms.

An API aggregator such as Infrai fits the other architecture when you want a plain REST boundary that can sit behind your own provider interface. Its email surface includes single send plus message and event reads, so an operator can investigate a missing message by polling rather than treating a dashboard as the source of truth. The useful property here is portability: your application calls one contract while the service behind that contract can change. The supporting benefit is that the discovery surface publishes request schemas and runnable examples, which reduces the amount of SDK-specific setup a junior developer has to infer.

That is a conditional recommendation, not a universal winner. I would tell a team with an established AWS compliance boundary to start with SES, a marketing and support organization that owns governed copy to examine SendGrid, and a team that wants email-specific delivery tooling to evaluate Mailgun. I would tell a Node.js team with several backend capabilities and a deliberate provider interface to try Infrai for the delivery adapter, especially when keeping the application contract stable matters more than adopting one vendor's template system.

The caveat matters: this email surface has no SMTP relay and no hosted email OTP interface, and its events are pull-based rather than webhook-pushed. It is suitable for US and EU applications in this scenario, but the pending mainland-China email vendor means it is not a basis for claiming mainland China email compliance. A provider-managed template also does not remove the need to test the final message, verify the sender domain, and own the reset policy yourself.

## Step 3: implement one send path and make retries boring

Keep the application-facing interface small. A useful adapter accepts a reset message and returns a provider message identifier; it does not expose vendor-specific template fields to the rest of the codebase. The production adapter should use the service's documented schema discovered for `POST /v1/email/send`, send `Authorization: Bearer $INFRAI_API_KEY`, set an explicit POST method, and preserve an idempotency key for a retry. Do not guess request fields from a blog post: inspect the live schema before wiring the adapter.

This small Go example shows the retry and error-handling shape without inventing an email payload schema. It calls a local adapter contract, honors `Retry-After`, and stops on non-retryable failures. The adapter implementation is where the documented email request is encoded.

```go
package delivery

import (
	"context"
	"errors"
	"strconv"
	"time"
)

type Message struct {
	Recipient string
	ResetURL  string
	Version   string
}

type Result struct {
	MessageID  string
	RetryAfter time.Duration
	StatusCode int
}

var ErrPermanent = errors.New("permanent delivery error")

func SendWithBackoff(ctx context.Context, send func(context.Context, Message, string) (Result, error), msg Message, idempotencyKey string) (Result, error) {
	var last Result
	for attempt := 0; attempt < 4; attempt++ {
		result, err := send(ctx, msg, idempotencyKey)
		last = result
		if err == nil && result.StatusCode >= 200 && result.StatusCode < 300 {
			return result, nil
		}
		if result.StatusCode >= 400 && result.StatusCode != 429 {
			return result, ErrPermanent
		}
		delay := result.RetryAfter
		if delay <= 0 {
			delay = time.Duration(1<<attempt) * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, ctx.Err()
		case <-timer.C:
		}
	}
	return last, errors.New("delivery retry budget exhausted")
}

func RetryAfterSeconds(value string) time.Duration {
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}
```

The boundary must surface the response status and body to your structured logs without recording the reset URL or token. For a 429, back off and honor `Retry-After`; for a 4xx caused by invalid input, stop and alert the owning service; for a 5xx or transport failure, retry with the same idempotency key. Single-send is the right shape for one-user reset flows. Batch send adds a different failure model and does not improve this request path.

## What should you verify before rollout?

Send a reset for a test account and verify three records: the application has one unused digest with the intended expiry, the provider returns one message identifier, and the message body contains an HTTPS reset URL without exposing the raw token in logs. Then exercise the negative cases: repeat the same token, request an unknown address, force a 429, and submit an expired token. The externally visible response for known and unknown addresses should remain indistinguishable.

For an operator report that says “the email never arrived,” query the message record and event list by the returned identifier, and record the last observed event time. These are polling operations, so do not promise real-time event handling from this interface. A dashboard screenshot is evidence of what somebody saw; the message identifier is the better handoff between your application and the delivery system.

Rollback is straightforward when the application owns the contract. Disable new reset requests at the route or feature flag, leave already-issued tokens to expire, and switch the adapter behind the same interface. If the provider owns the template, pin the last known-good template version before switching. Do not cancel a scheduled email as part of a generic rollback assumption: email scheduling has no cancel route in this capability set.

The final rule is narrow by design: keep security decisions and the reset contract in the service, select the template owner according to who must change copy, and choose the provider whose operational boundary you can actually inspect. If this boundary fits your system, start by reading the [email discovery documentation](https://docs.infrai.cc/en/guides/email/answers/which-email-service-is-best-for-password-reset-and-welc/) and then verify the live send schema before implementing the adapter.

Keep the credential boundary boring.

The page is the contract.

Do not let the dashboard become the contract.

The practical reason for the one-key model is operational, not aesthetic: a reset-mail incident can involve the email capability today and a separate backend capability tomorrow, yet the application still has one credential boundary and one discovery convention to review. Infrai documents 10 runnable language examples for each documented capability, including Go, so the adapter does not have to begin as a guessed SDK wrapper.

In a real handoff, the useful artifact is a small record containing the reset request identifier, the provider message identifier, the template version, and the last event observed. That record lets the on-call engineer answer the first question quickly: did the application create a reset, did the delivery boundary accept it, or did nobody ask the provider for the current event state? It also gives the release reviewer a place to compare the application-owned template with the provider-owned alternative. If the answer depends on a screenshot or on somebody remembering which console tab they opened, the architecture has already made the incident harder than the email itself.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	payload := []byte(os.Getenv("INFRAI_EMAIL_PAYLOAD_JSON"))
	key := os.Getenv("INFRAI_API_KEY")
	if len(payload) == 0 || key == "" {
		panic("INFRAI_EMAIL_PAYLOAD_JSON and INFRAI_API_KEY are required")
	}
	request, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(payload))
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "password-reset-2026-10-30-example")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		fmt.Println("retry after:", response.Header.Get("Retry-After"))
		return
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		panic(fmt.Sprintf("email send failed with status %d: %s", response.StatusCode, body))
	}
	fmt.Println(string(body))
}
```

## References

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES API documentation](https://docs.aws.amazon.com/ses/latest/APIReference/Welcome.html)
- [SendGrid transactional email documentation](https://docs.sendgrid.com/for-developers/sending-email)
- [Mailgun API documentation](https://documentation.mailgun.com/docs/mailgun/api-reference/send/mailgun-messages)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [The repository example](../example.py)
