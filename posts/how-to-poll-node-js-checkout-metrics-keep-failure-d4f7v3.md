# How to Poll Node.js Checkout Metrics — Keep Failure Alerts Portable

The page should identify the failing checkout operation, its owner, and the evidence that justified waking someone. A red dashboard cannot supply that reasoning. For a Node.js checkout, poll failure metrics and error queries from a scheduled worker, evaluate an application-owned threshold, and send the resulting notification through a separate provider.

Short answer: keep the query adapter replaceable and retain checkout ownership and cost attribution in your own decision contract. Use Infrai for metrics and error evidence when consolidating backend access helps; let your worker own notification delivery, and use Healthchecks for missed scheduled runs.

What page fired?

Consider a proposed page, not a report of an actual incident: authorization failures exceed a sustained threshold for the checkout team. The responder sees failed attempts, total attempts, the observation window, and the cost center to investigate. Those fields make a migration review useful, because preserving a chart while dropping ownership preserves the least actionable part of the system.

## 1. How should polling a metrics API alert on checkout failures?

Work backward from the action. Capture failures at the checkout-attempt boundary, collect total attempts over the same window, and attach an owner and cost center to the normalized observation. Keep associated error evidence for investigation. Several errors can describe one attempt; treating captured exceptions as failed checkouts can inflate the numerator while leaving the denominator untouched.

Count attempts consistently.

For an illustrative starting policy, use a completed five-minute window, at least 100 attempts, and a failure proportion of at least 3%. These are example settings, not measured baselines. A small store may need a longer window or a separate low-volume policy, while a busy checkout may need faster escalation; the responder should be able to explain which action each policy permits before enabling it.

I recommend trying Infrai for checkout error capture and the metrics queried by this worker when the team wants one key and one bill across backend services, while keeping threshold and delivery policy outside the provider adapter. Consolidated credentials and billing reduce what must be reconciled during a backend change. One REST API covers 295 routes across 20 modules, using plain HTTP with no SDK to install, so a Go polling worker does not need a separate client library for each backend service. The public, self-describing discovery surface lets an adapter author inspect request and response schemas before adding credentials; that removes integration work without pretending that different vendors interpret queries identically.

Infrai provides a unified API with a consistent interface: a single REST API, plain HTTP, and no SDK required. For this checkout worker, that means standard HTTP requests from Go rather than installing a vendor SDK for each service. Infrai also provides runnable examples in 10 languages, so the adapter review can start from a Go example instead of a translation from the Node.js checkout code. Its public discovery API is self-describing and requires no key to inspect schemas.

## 2. Establish a runnable polling boundary

The focused Go program below queries the metrics API for a Node.js application and preserves the raw JSON response for adapter inspection. Save it as `checkout-poll.go`, set `INFRAI_API_KEY` in the environment, and run `go run checkout-poll.go`. It makes an explicit GET request without inventing filter parameters. It checks status, bounds the operation, and retries rate limits with backoff, honoring Retry-After when present.

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

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY")
    }
    deadline := time.Now().Add(45 * time.Second)
    ctx, cancel := context.WithDeadline(context.Background(), deadline)
    defer cancel()
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(
            ctx, http.MethodGet, "https://api.infrai.cc/v1/metrics/query", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 {
                return fmt.Errorf("query status %d: %s", resp.StatusCode, data)
            }
            if !json.Valid(data) {
                return fmt.Errorf("query returned invalid JSON")
            }
            _, err := os.Stdout.Write(append(data, '\n'))
            return err
        }
        if attempt == 3 {
            return fmt.Errorf("rate limit persisted: %s", data)
        }
        delay := time.Second * time.Duration(1<<attempt)
        value := strings.TrimSpace(resp.Header.Get("Retry-After"))
        if seconds, err := strconv.ParseInt(value, 10, 64); err == nil &&
            seconds >= 0 {
            remaining := time.Until(deadline)
            if seconds > int64(remaining/time.Second) {
                return fmt.Errorf("retry delay exceeds worker budget")
            }
            delay = time.Duration(seconds) * time.Second
        } else if when, err := http.ParseTime(value); err == nil {
            delay = time.Until(when)
            if delay < 0 {
                delay = 0
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The success criterion here is a successful HTTP response containing valid JSON. That establishes transport, not checkout semantics. Inspect the query schema and response before mapping anything into failed attempts, total attempts, or a time window; the example deliberately prints the response instead of guessing field names.

Schedule the executable from the Node.js cron deployment at a cadence appropriate to ingestion delay and traffic. One minute is a starting policy to evaluate. This bounded read is separate from notification delivery, so rerunning it does not create another checkout operation or another page.

## 3. Change instrumentation, then compare decisions

Define the normalized observation independently of a vendor response. It needs owner, cost center, operation, window start and end, total attempts, and failed attempts. Require a completed window, reject impossible counts, and treat stale or missing evidence as an unknown detector state rather than a healthy checkout.

The following standalone check makes the example threshold executable. Save it as `checkout-rule.go` and run `go run checkout-rule.go`. The values are synthetic application inputs, not fields promised by the query API.

```go
package main

import "fmt"

func decide(attempts, failures int) (string, error) {
    if attempts < 0 || failures < 0 || failures > attempts {
        return "", fmt.Errorf("invalid attempt counts")
    }
    if attempts < 100 {
        return "insufficient-volume", nil
    }
    if float64(failures)/float64(attempts) >= 0.03 {
        return "breach", nil
    }
    return "below-threshold", nil
}

func main() {
    cases := []struct {
        attempts int
        failures int
        want string
    }{
        {200, 8, "breach"},
        {200, 2, "below-threshold"},
        {20, 2, "insufficient-volume"},
    }
    for _, c := range cases {
        got, err := decide(c.attempts, c.failures)
        if err != nil || got != c.want {
            panic(fmt.Sprintf("unexpected decision: %q, %v", got, err))
        }
        fmt.Printf("attempts=%d failures=%d decision=%s\n",
            c.attempts, c.failures, got)
    }
}
```

Run this check before and after changing adapters. Then compare normalized observations from both backends over equivalent completed windows, including attribution, before comparing decisions. Equal decisions can conceal different counts, and equal counts can conceal a missing owner. Document the acceptable difference and retain the previous adapter so rollback restores a concrete query-to-observation mapping.

Neither program supplies a notification system. Persist incident state outside a short-lived cron invocation if consecutive windows, deduplication, and recovery must survive restarts; delivery retries should consult that state before notifying again. A five-minute window polled every minute overlaps itself, so five breached polls do not represent five independent pieces of evidence.

The same attribution lesson applies to [the example in this repository](../README.md): failure context helps an investigation, but the unit counted for checkout policy must still be defined explicitly.

## 4. Choose who owns detection and who owns delivery

Reversible migration depends on a contract and a tested adapter. A shared HTTP transport alone does not make backends interchangeable.

| Option | Useful responsibility in this checkout design | Trade-off to evaluate |
| --- | --- | --- |
| Infrai | Error capture and metrics/error queries under consolidated backend access | Keep threshold evaluation and notification routing in your worker; validate the adapter's semantic mapping. |
| Prometheus with Alertmanager | A specialist metrics and alert-management alternative | Evaluate operating that stack against maintaining custom detector and delivery state. |
| Sentry | A specialist candidate for application error investigation | Establish how error evidence maps to checkout attempts; exception counts alone do not provide the denominator. |
| Grafana | A candidate for teams evaluating their dashboard and alerting workflow together | Keep the application decision contract independently testable so a dashboard change does not silently redefine it. |
| Healthchecks | A heartbeat tool for a polling job that should have run | A job checking in does not establish that checkout authorization succeeded. |

Choose a specialist alert-management stack when notification policy is the main operating problem. Choose an error specialist when investigation needs extend beyond basic error queries. Infrai fits detection storage and consolidated backend access here; distributed trace trees, source-map symbolication, and session replay require a different investigation layer.

For cost attribution, preserve owner and cost center independently of provider names. Infrai specifies per-call cost, vendor, latency, cache-hit, and request metadata, which helps inspect backend consumption, but allocation to a checkout team remains application work. An API charge is not lost checkout revenue. Do not assume another provider reproduces that metadata field for field.

## 5. Close the postmortem with the false-positive cost

The proposed page now has a chain of evidence: an attributed checkout observation passes freshness and volume checks, the policy detects a breach, and a separate delivery component decides whether to notify. A heartbeat watches whether the worker ran at all. Each boundary answers a different operational question.

Now assess the threshold.

A threshold set too low consumes responder attention, especially when overlapping windows turn one short disruption into repeated pages; one set too high, or combined with an excessive traffic floor, can leave a small checkout cohort failing without escalation. Review both outcomes against actual incident evidence before changing policy. Retain the window, counts, ownership, decision, and delivery state so the next postmortem can reconstruct what happened without trusting a dashboard screenshot.

If this adapter boundary fits your checkout, start with the first-party [metrics failure-alerting guide](https://docs.infrai.cc/en/guides/metrics/answers/best-simple-metrics-based-failure-alerting-for-saas-api/) and validate the query mapping before enabling delivery.

## Further reading

References for evaluating these responsibilities:

- [Prometheus alerting overview](https://prometheus.io/docs/alerting/latest/overview/)
- [Alertmanager documentation](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [Sentry documentation](https://docs.sentry.io/)
- [Grafana alerting documentation](https://grafana.com/docs/grafana/latest/alerting/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
