# Choose Node.js SMS Polling over Webhooks — For 3-Minute SaaS Compliance Alerts

Short answer: choose delivery-status polling for a low-volume Node.js SaaS compliance-notice path when a three-minute confirmation window is acceptable; choose webhooks when tighter latency or sustained volume makes repeated reads the larger operational risk.

For an edtech system sending a policy notice to students or guardians in the US and EU, "accepted by the SMS API" is not an auditable delivery record. The record needs a provider-neutral message ID, the intended recipient stored under the application's privacy rules, the notice revision, submission time, latest observed status, observation time, and the raw status category that produced the transition. Keep the notice itself out of operational logs. The API choice follows from whether that evidence can be collected predictably, not from how polished its dashboard looks.

This is deliberately a conditional choice. Polling is the simpler integration only while its bounded delay and read traffic fit the service objective; once they don't, webhooks should win.

## Why polling is the safer first operational choice

A webhook adds an internet-facing receiver, request authentication, replay handling, duplicate-event handling, out-of-order event handling, deployment coordination, and a place to retain failed deliveries. None of those jobs is individually exotic. Together, they create a second ingestion system whose failures can be mistaken for message failures. When an audit asks whether notice revision `policy-17` reached recipient record `guardian-4821`, a green chart cannot answer which state was observed, when it was observed, or why the application stopped checking.

Polling keeps ownership in one worker and one database transaction boundary. Submission stores the remote message ID before any status check is scheduled. A worker claims due records, asks for current status, maps that response into a small internal state machine, appends an observation, and schedules the next check only if the state is still nonterminal. The page should fire on an evidence gap: a record older than the confirmation objective with no terminal observation. It should not fire merely because one request was delayed.

That distinction matters at 3 a.m.

The catch is load. If notices become a high-volume stream, or if the product promise requires near-real-time status, repeated status reads waste capacity and widen the interval between a provider transition and the application's observation. Use authenticated webhooks in that case, while keeping the same append-only observation model. Polling also isn't suitable when the chosen API offers no status-read operation or makes status retention shorter than the application's check window; either change the integration pattern or reject that API during evaluation.

## How should a US/EU SaaS app poll SMS delivery status without webhooks?

Start with an explicit state model rather than copying provider strings through the application. `queued`, `submitted`, `delivered`, `undeliverable`, and `expired` are enough for the example, plus `unknown` as a visible classification failure. Terminal states never move backward. A newly encountered provider value must not be silently treated as success or failure; retain it, stop destructive follow-up, and route it to review. I'm not sure any fixed mapping will survive a provider contract change without maintenance, so the acceptance test is simple: replay the provider's documented status samples against the mapper before every deployment.

The worker below has no vendor route baked into it. Its transport is an interface, which lets the application test transition and audit behavior without pretending that every SMS API uses the same paths or response fields.

```go
package delivery

import (
	"context"
	"errors"
	"time"
)

type State string

const (
	Queued        State = "queued"
	Submitted     State = "submitted"
	Delivered     State = "delivered"
	Undeliverable State = "undeliverable"
	Expired       State = "expired"
	Unknown       State = "unknown"
)

type Observation struct {
	MessageID  string
	State      State
	RawStatus  string
	ObservedAt time.Time
}

type StatusReader interface {
	ReadStatus(ctx context.Context, messageID string) (raw string, err error)
}

type AuditStore interface {
	Append(ctx context.Context, observation Observation) error
	ScheduleNext(ctx context.Context, messageID string, at time.Time) error
}

type Worker struct {
	Reader StatusReader
	Store  AuditStore
	Now    func() time.Time
	Map    func(string) State
}

func (w Worker) Poll(ctx context.Context, messageID string) error {
	raw, err := w.Reader.ReadStatus(ctx, messageID)
	if err != nil {
		return err
	}

	state := w.Map(raw)
	if state == "" {
		state = Unknown
	}
	observed := w.Now().UTC()
	if err := w.Store.Append(ctx, Observation{
		MessageID: messageID,
		State: state, RawStatus: raw, ObservedAt: observed,
	}); err != nil {
		return err
	}

	switch state {
	case Delivered, Undeliverable, Expired:
		return nil
	case Unknown:
		return errors.New("unclassified delivery status")
	default:
		return w.Store.ScheduleNext(ctx, messageID, observed.Add(20*time.Second))
	}
}
```

The `20*time.Second` interval and three-minute objective are example policy values, not claims about carrier timing. Put them in configuration, add jitter so a batch does not synchronize its reads, and cap concurrent checks. On a retryable transport response such as HTTP `429`, honor the API's documented retry signal when one exists and do not append a fabricated delivery transition. On an authentication or contract error, stop the affected queue and alert once with the integration identifier; retrying an invalid request only turns one actionable page into noise.

## The API comparison belongs in a failure table

Do a small proof with the exact account region, sender configuration, and destination classes the application will use. Marketing checklists are weak evidence because "delivery status" might mean a terminal carrier observation, an intermediate handoff, or only API acceptance. Ask each candidate for its documented state definitions and retention window, then record what the application can prove from those fields. Your mileage may vary by destination and account configuration; the proof resolves that uncertainty better than a generic ranking.

| Decision point | Polling requirement | Webhook alternative | Reject when |
|---|---|---|---|
| Audit evidence | Status reads return a stable message ID, state, and observation data | Events carry the same correlation fields | Only submission acceptance can be retrieved |
| Integration effort | One authenticated outbound client and a scheduled worker | Public receiver, authentication, replay protection, and durable ingestion | Required security controls cannot be tested before launch |
| Failure isolation | Bounded retries, jitter, concurrency caps, and an age objective | Durable event receipt plus reconciliation reads | Missing updates cannot be detected independently |
| Regional handling | Account and data-flow choices can meet the application's US/EU policy | Receiver and event storage meet the same policy | Documentation cannot establish the required data path |
| Operations | Metrics come from the application's ledger | Metrics reconcile the event ledger with submitted IDs | The dashboard is the only source of history |

Don't score all rows equally. For this compliance-notice job, auditable evidence and failure isolation are gates; setup effort decides only among candidates that pass them. Price can be compared after a representative proof measures submission traffic, status-read traffic, retention needs, and operator workload, because a per-message figure alone leaves most of the polling system out of the comparison.

Also separate notification from authentication. NIST SP 800-63B discusses requirements and risks for authenticators; a compliance notice should not quietly become proof that the recipient authenticated. If email is a fallback channel, its domain-authentication work is separate too: DMARC policy and reporting are defined by RFC 7489, and an SMS delivery observation does not validate the email path.

## Verify the page before trusting the dashboard

Test the worker with controlled states: a normal terminal transition, a long nonterminal sequence, a duplicate observation, an unknown raw value, a rate-limited read, and a worker restart after the audit append but before scheduling. The database should enforce idempotency for the same message, raw status, and observation boundary. A restart must find records that are nonterminal and have no future check scheduled. This is the dull part of the system, which is precisely why it tends to be absent from product demos and present in postmortems.

Then run a synthetic notice through the production path without real compliance content. Verify three clocks: submission age, last successful status-read age, and time since the most recent terminal observation. Page on the first clock crossing the confirmation objective only when the second clock shows that evidence collection is unhealthy or the record remains nonterminal; otherwise create a lower-urgency workflow for an individual undeliverable recipient. One measures the pipeline. The other is a business outcome.

No mystery alerts.

The audit query should work without an API dashboard. Given a notice revision and recipient record, it should return the submission identifier and an ordered observation history, including unknown values. Redact phone numbers from alert payloads, keep access to the ledger narrow, and set retention from the organization's legal and privacy requirements rather than copying a provider default.

## Roll back the scheduler, not the evidence

Deploy the poller behind a per-tenant switch and begin with constrained concurrency. If read traffic or age metrics breach the rollout limits, disable new scheduling, let in-flight database writes finish, and preserve every observation already collected. Do not delete message IDs or rewrite terminal history during rollback. The previous worker version can resume from due nonterminal records because the durable ledger, not process memory, owns progress.

Moving later to webhooks should be a transport change, not a data-model rewrite. Feed authenticated events through the same mapper and audit append, retain a slower reconciliation poll for missing events if the selected API supports it, and compare submitted IDs with terminal observations. Choose polling now because it keeps this low-volume, three-minute workflow smaller. Stick with webhooks when latency and volume outweigh that simplicity.

## References

- RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- NIST SP 800-63B, Digital Identity Guidelines: Authentication and Lifecycle Management: https://pages.nist.gov/800-63-3/sp800-63b.html
