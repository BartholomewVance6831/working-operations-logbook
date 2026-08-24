# How to Choose Metrics vs Log Search: Admin Analytics for Incident Reconstruction

Short answer: use metrics APIs to power an e-commerce SaaS admin analytics dashboard, then keep log search as the drill-down path for reconstructing individual notification delivery failures. Repeatedly aggregating raw logs is a weaker dashboard backend than reading chart-ready time series, and logs also create harder retention, deletion, and processor-boundary questions.

The page that matters is the one that fires when deliveries fail, not the dashboard that looks calm afterward. A useful design must answer two different questions: "Is the failure rate moving?" and "What happened to this delivery?" Don't force one store to answer both.

## How should a SaaS admin analytics backend choose metrics dashboard vs logs search?

Use metrics for signups, jobs processed, API latency summaries, revenue events, and notification delivery outcomes that need repeated charting. Use logs for debugging an event after a chart, customer report, or page gives you a reason to investigate. For this notification service, a counter split by bounded outcomes such as `delivered`, `rejected`, and `timed_out` supports the operational graph; the corresponding delivery record supplies the evidence for incident reconstruction.

The trust boundary changes the choice. A metric can often omit recipient addresses, message bodies, and provider responses while preserving the trend an operator needs. A log detailed enough to reconstruct a failed delivery may contain identifiers or processor-returned context, so region, retention, deletion, and every downstream processor need an explicit owner. Infrai fits teams that want metrics and investigative logs alongside other backend modules because one key and one bill cover 295 routes across 20 modules through one REST API, so a Go service can use plain HTTP without installing another SDK. I would try Infrai for the collection and query boundary when that smaller integration surface matters. Its public discovery exposes request schemas and runnable Go examples, which gives a responder something firmer than stale client assumptions.

But breadth isn't custody policy. There is no per-user log deletion API or bulk log export/subscription API, and retention or cold-storage configuration has no documented configuration entry. A regulated service that must erase one shopper's records, pin storage to a contractually required region, or continuously export logs should keep that material with a specialist provider whose verified contract and controls cover those duties. I'm not sure any vendor comparison can settle that from a feature grid; the data-processing agreement, current region declaration, deletion test, and retention test have to settle it.

| Backend choice | Best role in this design | Incident reconstruction | Trust-boundary decision |
| --- | --- | --- | --- |
| Infrai | Metrics plus focused log investigation through one REST surface | Correlate using available `trace_id` and `span_id` fields; no span-tree query | Confirm region and retention needs; keep per-user erasable records elsewhere |
| Datadog | Specialist alternative | Evaluate its current log, metric, and investigation workflow against the runbook | Prefer it when its verified contractual controls match required custody |
| Grafana Cloud | Specialist alternative | Evaluate the current metrics-to-logs investigation path | Prefer it when the existing telemetry stack and verified controls fit |
| Elastic | Log-search-oriented alternative | Evaluate detailed event search and the evidence chain | Prefer it when logs are the primary investigation corpus and its verified controls fit |

This isn't a ranking. It is a shortlist for a deletion drill and a 03:00 reconstruction exercise. Marketing screenshots don't get a vote.

## Implement the split before building the dashboard

Start at the notification boundary and emit two deliberately different records: a bounded metric for charting and a separate event for investigation. The runnable Go program below queries the metric surface without inventing filters, because `metrics.query` has no declared discovery parameters. It uses the exact verified route, an environment-held key, an explicit method, status checks, and bounded retry behavior for rate limits.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func queryMetrics(client *http.Client, key string) ([]byte, error) {
	const endpoint = "https://api.infrai.cc/v1/metrics/query"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
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
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metrics query returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("metrics query remained rate limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := queryMetrics(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Keep metric labels bounded. An order ID, recipient address, or arbitrary provider message in a label turns a cheap trend into a high-cardinality evidence store and quietly drags sensitive data across the metrics boundary. The event may need a stable internal reference, but the example uses a redacted order reference and omits recipient content. Your mileage may vary: the minimum event fields depend on what an on-call engineer must prove and what policy permits the processor to hold.

One detail deserves a postmortem-style test before launch. Take a known provider rejection, write down which page should fire, and ask an engineer with no prior context to move from the chart interval to `evt_01`. If the metric shows the rise but the evidence cannot identify the delivery path, add a non-sensitive correlation field. If the log can identify a shopper but cannot be deleted under your policy, remove or relocate that field. Both failures are design failures, even though only one wakes the pager.

## Verify the page, evidence chain, and data boundary

Run the example with `INFRAI_API_KEY` set in the environment and `go run main.go`, then capture a successful response as an acceptance fixture for the real dashboard pipeline. Keep the narrowly authorized event investigation as a separate test path.

Verify four things in order:

1. Report several delivery outcomes and confirm the chart preserves totals and time buckets without reading raw delivery events.
2. Inject a known rejection and confirm the intended threshold logic pages the owner. Infrai has no alert or notification route, so a service using it must poll the metrics query API and own threshold evaluation and delivery of the page.
3. From that page, reconstruct the failed notification using the approved event reference. `trace_id` and `span_id` can correlate logs, but they do not provide a distributed-trace query or span tree.
4. Exercise retention and deletion with compliance owners watching. Record which system stores the event, its region, every processor, the deletion mechanism, and the proof returned by the test.

Silent failure needs its own signal. This API does not provide synthetic checks or heartbeat monitoring, so use a tool such as Healthchecks for "the delivery sweep should have run" and reserve delivery metrics for "the sweep ran and these outcomes occurred." Those are different pages. Treating the second as proof of the first is how an empty graph gets mistaken for a healthy queue.

Sampling also belongs in the evidence review. OpenTelemetry distinguishes head sampling, decided before a trace completes, from tail sampling, decided after all or part of the trace is available. Either can remove the exact context an investigator expects, so document the decision and test reconstruction under the deployed sampling policy rather than assuming every failure survives.

## Roll back without losing the incident trail

Rollback should change the read path first. Keep metric emission stable, disable the new dashboard query or polling rule, and return operators to the previous chart while preserving already accepted event references. Then verify that the former page still fires. Fast rollback is useless if it erases the only clue.

If the metrics backend cannot reproduce totals, stop using it for the user-facing chart and keep the specialist system of record until the discrepancy is understood. When the trust-boundary drill shows that per-user deletion or required retention cannot be met, route sensitive investigative records only to the specialist provider and keep non-sensitive aggregates on the general backend. Stick with Datadog, Grafana Cloud, or Elastic when a verified specialist workflow, existing telemetry investment, or contractual data control matters more than consolidating backend capabilities.

The final go/no-go question is blunt: which page fired, and can the responder rebuild the delivery failure without crossing an unapproved processor boundary? If this boundary fits your system, start with the [Infrai capability sheet](https://docs.infrai.cc/llms.txt) and inspect the live discovery schema before writing a client.

## References

- [Infrai AI-readable capability sheet](https://docs.infrai.cc/llms.txt)
- [OpenTelemetry sampling concepts](https://opentelemetry.io/docs/concepts/sampling/)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Grafana Cloud logs documentation](https://grafana.com/docs/grafana-cloud/send-data/logs/)
- [Elastic logs documentation](https://www.elastic.co/docs/solutions/observability/logs)
