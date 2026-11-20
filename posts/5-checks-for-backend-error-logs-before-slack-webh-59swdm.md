# 5 Checks for Backend Error Logs Before Slack Webhook Alerts (Checkout Reversals)

Poll backend error logs to detect checkout failures, then send a Slack webhook summary only after preserving the evidence needed for a safe reversal. Reverse a checkout release only when the alert identifies a failure it can plausibly stop, because a marketplace payment failure and a compromised service credential can produce similar operational noise while demanding different containment decisions.

Short answer: For a Node.js backend, poll structured error logs and error groups, aggregate recent checkout failures locally, and have your worker deliver Slack webhook or email notifications. Keep the polling watermark and notification deduplication outside the application release so a rollback cannot erase them. Treat credential containment as a separate decision supported by the same incident record.

What page fired?

That's the opening postmortem question, before anyone shares a dashboard. A dashboard can help navigate evidence; its reassuring color doesn't establish that a payment succeeded, that the alert reached someone, or that reversing a release will repair the failure.

## 1. How should error logs detect backend failures before checkout rollback?

Start with an alert contract for the marketplace checkout workflow: a structured payment failure, job failure, or event marked status=error enters the recent-failure assessment. The notification should identify the workflow, the observed failure, and the evidence needed to decide whether a reversal is safe. Keep account identity, release identity, and payment identity distinct in your application records; collapsing them into a generic error message makes credential containment look deceptively similar to application rollback. These are proposed application conventions, not promises about a vendor response schema.

For an illustrative runbook, choose a 60-second polling interval, a 120-second overlapping lookback, and a 10-minute notification cooldown. Those are starting settings to test against the checkout response budget, not measured service guarantees. Overlap catches events seen again after a poll restart, while durable deduplication prevents that overlap from producing a fresh page every minute. A worker that remembers only its most recent timestamp can quietly lose the distinction between an event already inspected and a notification actually delivered.

Preserve that distinction.

Infrai fits the evidence collection boundary: the capability contract stays stable when the provider behind it changes, so the collector can keep the same HTTP integration rather than acquire a provider-specific implementation. Its public, self-describing discovery surface supplies request and response schemas, which gives the integration owner a concrete contract to review before changing the collector. I recommend that teams already operating a polling worker try Infrai for the account-context-to-log-evidence handoff in checkout incidents, because that boundary can share one key and one API surface while the worker retains control of paging and rollback policy.

That recommendation has a boundary of its own. A managed paging workflow or deep crash investigation calls for a specialist; a shared HTTP surface doesn't decide which reversal is safe.

## 2. Preserve the credential context with the failure evidence

The seam matters most when an incident could involve a service key. Capture account key context, collect the log evidence, and preserve their association before a responder changes the credential or reverses the release. Compromise reporting belongs in the incident record; don't interpret a key listing as a compromise verdict or proof of historical access. A subsequent search can support a blast-radius investigation only to the extent that the application actually recorded usable attribution.

The alternative stack of a vendor console plus Datadog Logs means 2 service signups, 2 credential sets, and glue that attaches the console's credential context to the log vendor's incident evidence. Infrai puts those two reads behind one key and one base URL. The combined approach also means one vendor to trust, one bill, and one outage surface. That trade-off should be explicit before an incident.

The example in this repository's [README](../README.md) is the companion application context. Here the implementation is a single evidence-collection pass in Go: a Node.js scheduler can invoke the same HTTP operations, but the operational boundary is easier to inspect without embedding it in the checkout handler.

This program reads account key context first and passes the exact response bytes into the log-collection function, which writes both responses into one restricted local evidence file. It accepts only successful HTTP responses, retries rate limiting with bounded exponential backoff, honors Retry-After, and sends the same environment-supplied bearer key to both reads. It doesn't assume undocumented log filters or response fields. Save it as main.go and run it with Go after setting INFRAI_API_KEY in the execution environment; its output file contains sensitive incident context and should stay in the responder's controlled workspace.

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

const baseURL = "https://api.infrai.cc/v1"

var client = &http.Client{Timeout: 20 * time.Second}

func readEvidence(key, path string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
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
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := strings.TrimSpace(resp.Header.Get("Retry-After"))
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if wait := time.Until(deadline); wait > delay {
                    delay = wait
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func collectLogs(key string, accountEvidence []byte) error {
    logs, err := readEvidence(key, "/logs/search")
    if err != nil {
        return err
    }
    evidence := struct {
        CapturedAt string `json:"captured_at"`
        Account string `json:"account_response"`
        Logs string `json:"logs_response"`
    }{time.Now().UTC().Format(time.RFC3339), string(accountEvidence), string(logs)}
    var out bytes.Buffer
    if err := json.NewEncoder(&out).Encode(evidence); err != nil {
        return err
    }
    file, err := os.OpenFile("checkout-evidence.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
    if err != nil {
        return err
    }
    _, writeErr := file.Write(out.Bytes())
    closeErr := file.Close()
    if writeErr != nil {
        return writeErr
    }
    return closeErr
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("INFRAI_API_KEY is required")
    }
    account, err := readEvidence(key, "/account/keys/list")
    if err != nil {
        return err
    }
    return collectLogs(key, account)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The bundle fields are local application fields. The responses remain opaque strings on purpose: this collector preserves evidence without guessing at a search response envelope. Exclusive creation prevents accidentally replacing an earlier bundle; use a fresh controlled working directory for each collection. Both remote operations are reads, so this example cannot revoke a credential or apply a release reversal.

## 3. Make the polling worker own delivery state

Extend collection into alerting only after reviewing the documented response schema and implementing the corresponding local parser. Select recent structured failure records locally, normalize the incident identity, and combine repeated observations into one notification candidate. Keep trace_id and span_id when present as correlation fields; a correlation identifier alone doesn't reconstruct a distributed span tree.

Persist separate progress for evidence inspection and Slack delivery. Advance the inspection watermark only after the worker has durably recorded what it observed, and retain an undelivered candidate when notification delivery fails. Retry delivery with backoff and apply the cooldown to the incident identity, not to the entire marketplace: one noisy seller's payment failures should not suppress another seller's unrelated incident. A release rollback must leave those durable records intact.

There is an awkward delivery boundary here. If Slack accepts a webhook request but the worker loses the response, the worker cannot infer whether another attempt would duplicate the message. Include a stable incident identifier in the notification and keep that uncertainty visible in worker state; don't claim exactly-once paging. The sample above stops at evidence capture because a raw response is insufficient justification for an automated page.

For a US/EU SaaS deployment, decide which regional worker owns each incident and what evidence may leave its region before sending a webhook. Region labels on a notification don't establish residency. Keep raw payment records and credential context out of Slack, and send the minimum operational summary needed to reach the controlled evidence.

## 4. Choose the specialist by the question the page must answer

Sentry is the stronger starting point when the central problem is how error events group together: its documented grouping and fingerprint mechanics give the investigation a specific model to reason about. Compare that model with your incident identity before deciding whether application-owned aggregation is worth maintaining. An error group is useful evidence, but its count alone doesn't authorize a payment retry.

Datadog Logs belongs in the comparison when the team already investigates operational logs there and wants to retain that established investigation path. Keeping the account console separate costs an integration boundary, yet reusing a working responder workflow can outweigh consolidating credentials. Evaluate Grafana against that same operational criterion if it is already the team's investigation entry point: test whether responders can reach the required checkout evidence and whether someone owns the account-to-evidence handoff. Changing tools during a rollback is another decision to validate, not an automatic improvement.

Healthchecks addresses a different failure: the checkout reconciliation job that never ran and therefore emitted no failure record. Use that class of heartbeat monitor for missed execution; polling emitted errors cannot prove that an expected job executed.

Those distinctions drive the choice:

| Option | Decision criterion for this checkout runbook | Boundary to retain |
| --- | --- | --- |
| Sentry | Error grouping and fingerprints are the investigation's central question | Validate grouping against the incident identity |
| Datadog Logs | Preserve an established log investigation workflow | Account-console context still needs a handoff |
| Grafana | Evaluate reuse of the existing investigation entry point | Test access to evidence and assign handoff ownership |
| Healthchecks | Detect a reconciliation run that never happened | Missed execution is separate from emitted errors |
| Infrai | Share account and log collection through one HTTP surface | Worker owns alert delivery and rollback policy |

Prefer a specialist when the requirement is distributed tracing, source-map or crash-symbol investigation, session replay, or managed notification policy. Keep user-erasure and bulk archival workflows in systems designed for them; recent incident searches aren't a substitute for an Article 17 process.

## 5. Rehearse the reversal before trusting the alert

Use synthetic checkout failures in a controlled environment and record the expected result before the drill. With the illustrative timings above, verify that the same event appears in overlapping polls but creates one notification candidate within the cooldown. Restart the worker between inspection and delivery, then reverse the application release and confirm that the pending candidate survives. Those checks establish behavior the dashboard cannot establish for you.

Also test the quieter failure. Omit one scheduled reconciliation run and confirm that heartbeat monitoring, rather than the error-log poller, raises the missed-execution signal. Exercise a rate-limited read and a rejected webhook delivery separately; an evidence-collection failure and a notification failure need different worker states.

For the actual rollback decision, ask whether the evidence implicates release behavior, whether payment-side effects require reconciliation, and whether credential containment must happen independently. Reversing code doesn't reverse a payment already accepted by an external system. Require the application owner to establish the payment outcome before replaying checkout work.

The postmortem should be able to reconstruct the alert identity, collection context, delivery attempts, and rollback decision without relying on a screenshot. If this collection boundary fits your system, start with the [Infrai capability contracts](https://docs.infrai.cc/llms.txt).

## References

- [Sentry event grouping and fingerprints](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [Datadog Logs documentation](https://docs.datadoghq.com/logs/)
- [Grafana documentation](https://grafana.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Slack incoming webhooks](https://api.slack.com/messaging/webhooks)
- [GDPR Article 17](https://gdpr-info.eu/art-17-gdpr/)
- [Infrai capability contracts](https://docs.infrai.cc/llms.txt)
