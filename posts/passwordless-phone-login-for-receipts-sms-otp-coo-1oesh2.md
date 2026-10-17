# Passwordless Phone Login for Receipts — SMS OTP Cooldown and Attempt Control

At 03:12, the page for passwordless phone login in an Express Node.js receipt service says `receipt_login_failures_high`. The on-call can see 1,840 failed SMS OTP verifications in five minutes, but not whether people are mistyping codes, delayed messages are arriving after a resend, or one caller is cycling phone numbers. That page cannot support a safe action. It reports an outcome after the system has already annoyed customers and may have amplified traffic to the SMS sender.

**TL;DR:** For a media store that reveals an order receipt after payment settles, model passwordless phone login as a server-side challenge state machine. Store only a keyed digest of the code, make issuance and verification atomic, keep resend cooldown separate from code expiry, cap both guesses and sends, and instrument decisions rather than one aggregate failure count. In an Express/Node.js service, the HTTP handlers should be thin wrappers around those storage operations; the security property lives in the state transition, not the framework middleware.

The earlier signal should have been a rising ratio of resend requests denied by cooldown, split from provider submission failures and invalid-code attempts. Those three events demand different responses. Combining them makes a colorful dashboard and a useless page.

## How should Express Node.js handle passwordless phone login SMS state?

Start with actions. An on-call can disable a route, tighten an abuse rule, investigate a delivery dependency, or do nothing. A page should distinguish those choices before it wakes anyone.

For this receipt flow, emit one structured event after each server-side decision. Useful fields are `challenge_id`, a pseudonymous account or phone key, `order_id`, `action`, `outcome`, `reason`, `challenge_age_bucket`, `send_count`, `attempt_count`, and a correlation ID. Do not put the phone number or OTP in logs. Keep the provider's message identifier in restricted telemetry if it is needed to reconcile submission and delivery events; it does not belong in broad application logs.

The first instrumentation change is to replace `login failed` with a small reason vocabulary: `invalid_code`, `expired`, `attempt_limit`, `resend_cooldown`, `send_limit`, `provider_rejected`, and `state_conflict`. Alert on symptoms that map to an operator action. A surge in `resend_cooldown` is evidence about user behavior or delivery latency, while `provider_rejected` points at the delivery boundary. Neither proves the other. No single threshold is universally correct. Establish a baseline by route and market, require a minimum event volume, and page only when both the rate and duration imply material receipt-access impact. A low-volume anomaly can open a ticket. This is deliberately conservative: a threshold tuned to catch every brief spike will train the on-call to distrust the one alert that matters, while an insensitive threshold leaves paid readers staring at a locked receipt.

That is the trade-off.

## Put the invariant in one atomic transition

The challenge record is the unit of consistency. A practical record contains a random challenge ID, phone-key digest, order binding, code digest, creation and expiry times, next-send time, send count, failed-attempt count, status, and version. The order binding matters because authenticating a phone number must not silently authorize every receipt that a caller can guess; authorization still checks that the settled order belongs to the authenticated subject.

Do not store the code in plaintext. A six-digit space is too small for a plain fast hash to resist offline enumeration, so compute a keyed digest with a server-held secret and include the challenge ID in the input. Compare digests in constant time. Protect that secret like other authentication key material and define how active challenges behave during rotation.

The following Go sketch shows the boundary, not a complete service. The storage methods must implement compare-and-swap or transactions; replacing them with a read followed by a write reintroduces the race the interface is meant to prevent.

```go
package otp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"time"
)

var (
	ErrCooldown    = errors.New("resend cooldown active")
	ErrSendLimit   = errors.New("send limit reached")
	ErrAttemptLimit = errors.New("attempt limit reached")
	ErrInvalidCode = errors.New("invalid code")
)

type Challenge struct {
	ID            string
	OrderID       string
	PhoneKey      [32]byte
	CodeMAC       [32]byte
	ExpiresAt     time.Time
	NextSendAt    time.Time
	SendCount     int
	FailedAttempts int
	VerifiedAt    *time.Time
	Version       int64
}

type Policy struct {
	CodeTTL       time.Duration
	ResendAfter   time.Duration
	MaxSends      int
	MaxAttempts   int
}

type Store interface {
	// ConsumeAttempt atomically rejects terminal challenges, increments a failed
	// attempt, or marks the challenge verified when the MAC matches.
	ConsumeAttempt(ctx context.Context, id string, now time.Time, codeMAC [32]byte, maxAttempts int) error
	// ReserveResend atomically enforces NextSendAt and MaxSends before returning.
	ReserveResend(ctx context.Context, id string, now time.Time, next time.Time, maxSends int) error
}

func codeMAC(secret []byte, challengeID, code string) [32]byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(challengeID))
	h.Write([]byte{0})
	h.Write([]byte(code))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
```

Why insist on atomicity? Two concurrent guesses can both observe `failed_attempts = 4`, both pass a five-attempt check, and both write back `5`. Concurrent resend requests have the same problem: they can each observe an elapsed cooldown and each submit a message. A process-local lock does not fix this once there is more than one application instance.

Use a unique idempotency key for the logical send reservation. Commit the reservation before calling the delivery adapter, then record the submission result. That ordering may require a reconciler for reservations whose process died before submission, but it prevents parallel handlers from treating one user action as several permitted sends. The alternative ordering can send a message and crash before recording it, which makes an immediate retry indistinguishable from a new send.

## Repeat sends must preserve the security state

A resend endpoint should not create a fresh, unlimited guessing budget. Decide explicitly whether a resend keeps the current code or rotates it. Keeping it reduces confusion when messages arrive out of order, but extends exposure to the same code until its original expiry. Rotating it invalidates an intercepted older code, but delayed delivery can make a user enter a code that was valid seconds ago.

For receipt access, I would start with one active challenge per phone-and-order pair, preserve the original absolute expiry across resends, and keep the failed-attempt count. That is a design choice, not a universal constant. It favors predictable bounds and simpler incident reconstruction over giving every delayed message a fresh lifetime.

Pick concrete policy values in configuration, then test their interaction. The boundary-test matrix needs cases immediately before, exactly at, and immediately after expiry and cooldown, followed by concurrent requests at each configured send and attempt limit. Production values come from delivery data, support load, threat modeling, and accessibility review, not a number copied from an example. The critical invariant is that a repeat send cannot extend expiry or erase attempts by accident.

Short codes are rate-limited credentials. Apply limits at several scopes: challenge, pseudonymous phone key, account or order, source network, and broader traffic segment. No one dimension is sufficient. IP-only controls punish carrier-grade NAT users and are easy to distribute around; phone-only controls let an attacker fan out over many destinations. Avoid exposing whether a phone number has an account or a settled order. Return a uniform acknowledgement while recording the internal denial reason.

## Keep handlers boring and adapters replaceable

In Express, parse and validate input, call the challenge service, map its typed result to a stable response, and emit the decision event. Do not let the route generate codes, maintain an in-memory cooldown map, or infer success from a delivery API's immediate response. A sender accepting a request is not the same event as a handset receiving a message.

The delivery interface can stay small: submit a destination, message body, and idempotency key; return a provider reference and a classified outcome. Normalize dependency errors into categories the policy understands, such as retryable transport failure, permanent destination rejection, and ambiguous submission. Keep raw responses for restricted diagnostics, with retention appropriate to their sensitivity.

SMS length also crosses this boundary. GSM-7 messages have a different single-message character limit from UCS-2 messages, and concatenated messages reserve characters for segmentation. Twilio's technical explanation documents the 160-character GSM-7 and 70-character UCS-2 single-segment limits, plus the reduced limits for concatenated segments. Even with a different carrier, test the exact localized template and encoding; an unexpected Unicode character can change segmentation and therefore delivery behavior.

This architecture has limitations. SMS depends on phone-number control and a delivery network outside the application, so it is a poor fit where the threat model requires phishing-resistant authentication; NIST SP 800-63B explicitly says PSTN out-of-band authentication is not phishing-resistant and treats use of the PSTN as a restricted authenticator. It is also a poor fit when users cannot reliably receive texts. In those cases, select an authenticator appropriate to the assurance requirement rather than weakening the retry policy until messages appear convenient. The integration-effort trade-off is real: a small SMS adapter is easy to isolate, but recovery, number changes, accessibility, and stronger-authentication enrollment still require product and operational work.

Emailing the settled order receipt is a separate delivery path, not an authentication fallback that should bypass the phone challenge. A transactional email service such as Amazon SES can carry the receipt, while access to the web copy still follows the authorization rule. Keeping those concerns separate makes integration easier to test and prevents a failure in one channel from mutating authentication state in another.

## Test races, deploy policy slowly

Unit tests are the easy layer. They should cover expiry boundaries, a resend exactly at `next_send_at`, the last permitted attempt, a correct code after the cap, and a resend after verification. Use a fake clock and deterministic code generator so these tests do not sleep.

The tests most likely to prevent an incident are concurrent integration tests against the real storage engine. Fire 20 resend requests at one challenge and assert that only one reservation succeeds. Fire simultaneous wrong guesses around the cap and assert that the stored count never loses an increment. Then inject a process failure after reservation but before submission and verify that reconciliation reaches a known terminal state without issuing an unbounded second message.

Deploy telemetry before enforcement. Observe the proposed reason counters without denying traffic, check cardinality and privacy, then enable limits for a small slice. Policy changes need version labels in events because a graph spanning two policies is otherwise hard to interpret. Roll back the policy independently of the application binary. The receipt itself supplies the end-to-end service-level signal: among settled orders whose owners request access, how many reach an authorized receipt view within the chosen time window? Delivery submissions and OTP verifications are supporting signals. They are not the customer outcome. The tempting assumption is that a rising verification count proves the flow is healthy. It does not: retries can inflate that count while fewer people reach the receipt.

Watch the outcome.

The final alert should say which policy version is active, which reason crossed its baseline, whether settled-receipt access is degraded, and what action is available. Get the threshold wrong on the sensitive side and normal retries page the team; get it wrong on the quiet side and customers lose access while every dashboard remains green. Measure both costs, but reserve paging for sustained, actionable impact.

## Further reading

- [NIST SP 800-63B: Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Twilio: SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
