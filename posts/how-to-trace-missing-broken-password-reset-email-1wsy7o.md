# How to Trace Missing Broken Password Reset Email Links: HTML Encoding Evidence

**Short answer:** For a missing or broken email link, keep a durable receipt record and alert on the absent delivery transition; the same evidence trail handles a password reset issue without treating a dashboard as proof.

The least complex design is a durable receipt record plus one alert that fires when a settled payment has no accepted delivery event. Send the receipt from a queue, retain the rendered message and provider response identifiers, and make the alert point to that record. A dashboard showing a green send rate is not evidence.

For a property-management system, the question is concrete: after payment settles, can the team prove which receipt was generated, which address received it, what the mail system accepted, and what happened when delivery did not complete? That chain matters more than a clever template or a high-level percentage.

## What page fired at 3 a.m.?

Start with the page an on-call engineer actually sees. It should identify a property, an order, a payment event, and the missing transition, for example: `receipt_delivery_pending`, order `ord_7842`, settled 11 minutes ago, recipient hash `sha256:...`, attempts `2`. It should link to an immutable event view, with the tenant's sensitive address withheld from the alert itself.

The useful trace runs forward and backward:

1. The payment service records `payment.settled` with an idempotency key.
2. A receipt worker claims that event and stores a versioned HTML and plain-text rendering.
3. The mail adapter records the submission response, timestamp, message identifier, and sanitized error class.
4. A delivery consumer records accepted, deferred, bounced, or complained events when the sending system exposes them.
5. A timer pages only when the expected next event is absent beyond a stated interval.

The page is the last link in that chain. Work backward from it. If the receipt worker has no claim event, the problem is queue ownership. If it has a claim but no submission, inspect the adapter. If submission exists but acceptance does not, inspect the mail boundary and its response. If acceptance exists but the resident reports nothing, the investigation moves to filtering, mailbox rules, or a bad recipient record. Those are different incidents and should not share one generic `email_failed` label. The same troubleshooting split applies to a password reset email, but the authorization and token policy belong to that separate workflow.

A false positive still has a price. Page too early and the team starts duplicate sends while the queue is merely slow; page too late and a resident waits without a receipt. Pick the interval from observed queue and mail-boundary behavior, record the reason, and review it after an incident.

That is the page.

## How do password reset email links become missing or broken?

The first question is not which email client displayed the message. It is whether the generated artifact contained a valid link before the client received it. An HTML template issue, URL encoding error, or client rewrite can look identical to a resident. The event record separates them.

## Which evidence should survive a disputed receipt?

A message body alone does not prove delivery. It proves what the application intended to send. The evidence model should separate intent, submission, and downstream outcome. That separation also keeps an operator from claiming that a provider acceptance means the recipient read the message.

For every receipt, keep a correlation ID, order ID, payment-settled timestamp, template version, rendering checksum, destination-domain classification, attempt number, and event timestamps. Keep the minimum personal data needed to investigate: a keyed or one-way address fingerprint is usually more useful in alerts than a full address. Define retention and access rules with the people responsible for the property records; the engineering team should not quietly invent a legal retention period.

The rendering checksum catches a class of failures that delivery metrics miss. A deployment can successfully submit a message whose call-to-action has an empty `href`, a URL with an unescaped ampersand, or a template branch that omits the receipt number. Store the rendered artifact under the same correlation ID as the payment event, and test both HTML and plain text before enqueueing.

A minimal event record can look like this:

```json
{
  "correlation_id": "rcpt_7842_a1",
  "order_id": "ord_7842",
  "payment_state": "settled",
  "template_version": "receipt-v7",
  "render_sha256": "...",
  "attempt": 1,
  "events": [
    {"type": "generated", "at": "2026-10-29T03:14:02Z"},
    {"type": "submitted", "at": "2026-10-29T03:14:04Z", "message_id": "..."}
  ]
}
```

Do not put a reset token, payment credential, or complete mailbox address into logs. A receipt link should be an authorization-checked, expiring reference to the order record, not a bearer copy of the payment details. That is a security decision as much as a mail decision.

## How do you test the boundary before it pages?

The preflight test belongs between template rendering and queue publication. It should reject malformed links, missing order data, and a mismatch between the HTML and text parts. It should also exercise the same escaping path used in production; a test that assembles a URL by string concatenation is testing a different program.

Here is a deliberately small Go validator for a receipt link. It accepts only HTTPS, requires a host, and uses structured URL fields so query values are escaped by the standard library. The allowlist is an application policy, not a claim that every property system uses the same host.

```go
package receipt

import (
	"errors"
	"net/url"
	"strings"
)

func ReceiptURL(base, orderID, proof string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", errors.New("receipt base must be an HTTPS URL")
	}
	if strings.TrimSpace(orderID) == "" || strings.TrimSpace(proof) == "" {
		return "", errors.New("receipt identifiers are required")
	}
	q := u.Query()
	q.Set("order", orderID)
	q.Set("proof", proof)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
```

The test cases should include spaces, ampersands, Unicode property names, an already encoded value, an empty identifier, and an unexpected scheme. Then parse the returned URL and assert on query values rather than comparing a hand-written string. Render the result into the actual HTML attribute, parse the HTML, and assert that the anchor has one non-empty HTTPS target. A browser preview is useful for humans; it is not a substitute for these assertions.

Deployment needs one more check: compare the new template version's render fixtures with the previous version, including the plain-text part. Roll back the template independently of worker code when possible. This is an operational boundary worth preserving because a receipt may be legally or operationally important even when the payment path itself is healthy.

## What should the alert prove, and what should it avoid?

Instrument transitions, not vague success counters. Useful metrics include counts by event type, age of the oldest unsettled delivery, retry age, and rejection classes. Keep dimensions bounded: property region, destination-domain class, template version, and failure class are more actionable than an unbounded order ID label. Put correlation IDs in logs and traces, where an operator can search them.

The alert should have a runbook sentence for each branch: inspect the event record, check whether the worker claimed the payment event, check the adapter response, and suppress a retry when the event is already accepted. A retry must be idempotent. The receipt record needs a stable send key such as `order ID + receipt version`, otherwise a transient timeout can produce two receipts and two pages of confusion.

Mailbox behavior is outside the application boundary. Gmail and Yahoo both publish sender guidance covering authentication, reputation, and message handling; those requirements can affect whether a message reaches a mailbox, but neither an application log nor a submission response proves that a person saw it. Treat provider acceptance as one evidence state, then capture later delivery signals where available.

The limitation is important: application evidence cannot prove that a resident opened a receipt, and a client-side report cannot prove the original HTML was broken. Keep those claims separate. A self-hosted mail boundary gives more control over records, while a managed boundary may provide downstream signals the application would otherwise have to collect; the choice depends on evidence access, operational ownership, and the team's ability to run it.

I would rather have a smaller alert with a precise missing transition than a wall of delivery percentages. At 3 a.m., the first useful fact is not that 99.8% of receipts were submitted. It is which settled payment has no next event, what the last known event was, and whether retrying can create a duplicate.

## Further reading

- Google, Email sender guidelines: https://support.google.com/a/answer/81126
- Yahoo, Sender best practices and requirements: https://senders.yahooinc.com/best-practices/
- RFC 3986, Uniform Resource Identifier syntax: https://datatracker.ietf.org/doc/html/rfc3986
- Go `net/url` package documentation: https://pkg.go.dev/net/url
