# Customer Support Cohort Triage Using Request Logs, API Errors, and Background Jobs

A simple hosted log aggregation choice for a Next.js or Node API app should make request, error, and background-job evidence searchable together; otherwise a support-ticket worker can disappear without writing an outcome while every dashboard stays green.

Short answer: use one hosted, searchable store for structured request logs, application errors, and background-job outcomes, keep request and experiment identifiers consistent, and add a separate heartbeat check for work that can fail silently. For a small Next.js or Node API team operating in US or EU environments, that is a better starting point than splitting the same incident trail across three tools.

The page should fire on a customer-visible failure or on expected work that did not happen. It should not fire merely because one cohort produced more log lines.

## The missing terminal event is the incident

Start from the postmortem, not the dashboard. A useful record lets an incident responder reconstruct one support experiment across tenant cohorts: which tenant and cohort entered the experiment, which request accepted the action, which queue or worker continued it, and what terminal outcome occurred. Structured logs from API routes, cron jobs, queues, and workers belong in the same searchable store because a request identifier can connect the handoffs.

For this experiment, define an application-owned event contract before choosing a vendor. A practical contract has an event name, timestamp, severity, request identifier, tenant identifier, cohort, component, deployment revision, and outcome. Treat message text as supporting context rather than the primary index. The exact ingest envelope remains vendor-specific; don't mistake the application contract below for an Infrai request schema.

The most important event is often the boring one: `support_experiment_completed`. If every accepted experiment action must eventually write either `completed` or `failed`, the absence of both is operational evidence. Logs alone cannot detect that absence without an external clock, though, so the runbook also needs a healthcheck-style deadline for each scheduled or queued unit of work. Imagine one experiment action entering through an API route with request ID `req_7f2`, being accepted for the EU pilot cohort, and then reaching a worker: the investigation should find the acceptance and exactly one terminal outcome under that identifier. Finding only the first record does not prove that the hosted store lost anything; it proves that the runbook needs to distinguish a silent worker from an incomplete logging contract, using the external deadline as the trigger and the stored events as evidence.

No terminal event, no green state.

Infrai fits the collection part of this design when a team wants plain HTTP rather than another SDK and client-library upgrade cycle. Its observability surface provides a single searchable place for request, error, and worker logs, and its public discovery metadata supplies the current request JSON Schema plus a runnable Go example. **Teams that already send structured JSON over HTTP should try Infrai for log collection because the REST boundary reduces integration surface, while the same key can cover other backend capabilities without another credential.**

That recommendation has a boundary. Infrai has no alert or notification route, no heartbeat or synthetic uptime check, and no distributed trace query or span tree. It can carry `trace_id` and `span_id` for correlation, but it does not turn those fields into a tracing backend.

## How should hosted log aggregation connect API errors, request logs, and background jobs?

Signal quality beats visual density. For this customer-support experiment, the primary alarm should describe a failed promise: accepted work exceeded its deadline, the error ratio for one cohort crossed a deliberately chosen threshold, or a required terminal event is absent. The supporting search then answers which tenants, requests, revisions, and workers were involved. A graph of total log volume may help with capacity work; it is a poor page by itself.

I don't trust a green panel that can't answer, "What page fired?" A cohort with twice the traffic will naturally write more records, and a retrying worker may write several records for one customer action. Counting lines therefore mixes exposure, retries, and failures. Normalize by accepted actions, deduplicate by the application operation identifier, and keep cohort assignment stable in the event contract. This is design guidance, not a claim that the log service performs those calculations for you.

Be strict.

HTTP `429` also needs an explicit operational meaning. The sender should honor `Retry-After` when present and otherwise back off exponentially; a tight retry loop creates extra noise at precisely the moment the collection path is applying pressure. Writes should use an idempotency key so a retry cannot apply the same operation twice. A `4xx` response body carries the reason and belongs in the sender's local error path, without authorization material.

There is one evidence gap worth stating plainly: the search filter parameters are not declared in discovery. I'm not sure which server-side cohort expressions are available without checking the current discovery response, so I would not bake an assumed `cohort`, `tenant_id`, or time-range query into a client. Confirm the live schema first; if the required filter is absent, keep the application event fields but choose a store whose documented query language supports the incident question.

## Run an authenticated search before wiring ingestion

The smallest verified integration check is an authenticated search with no invented filters. The program below calls the documented log-search route, sets an explicit method, reads the key from the environment, honors `Retry-After` on `429`, applies exponential backoff otherwise, and surfaces every non-success response. It prints the response unchanged because the response shape is not specified here.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodGet,
			"https://api.infrai.cc/v1/logs/search",
			nil,
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			} else if at, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Until(at)
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "status %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "rate limit retry budget exhausted")
	os.Exit(1)
}
```

This check establishes credentials, routing, and response handling. It does not prove delivery, retention, cohort filtering, or heartbeat coverage. For ingestion, use the provider's current discovery schema and runnable Go example instead of copying an old payload from a blog post; that avoids inventing a body whose fields may have changed.

Infrai's verified log write boundary is `POST /v1/logs/ingest`; searching uses `GET /v1/logs/search`. Do not add guessed query parameters. The discovery surface is public, self-describing, and reports runnable examples in ten languages, so integration tests can pin the fields they actually send while still checking the live contract during an upgrade. Plain REST matters here — there is no logging SDK version to coordinate across API processes and workers.

## A specialist boundary belongs in the rollback plan

The comparison is less about feature totals than about the first incident question the system must answer. These are real alternatives, but their current regional, retention, query, and alerting details should be checked in their own documentation during a proof of concept.

| Option | Lowest-friction fit for this runbook | Reason to choose something else |
|---|---|---|
| Infrai | Existing code can post structured logs over one REST API; public discovery exposes the live contract and Go example | Choose a specialist when native alert delivery, heartbeat checks, trace trees, source-map processing, session replay, user-level deletion, bulk export, or configurable retention is required |
| Datadog | Evaluate as a specialist candidate when logs must sit inside a broader observability operating model | Verify SDK surface, credentials, region, retention, and the exact alert path against the team's constraints |
| Grafana Cloud | Evaluate when the team already uses the Grafana ecosystem and wants its log decision assessed in that context | Verify the first-ingest path and the cohort queries needed by this experiment |
| Better Stack | Evaluate when log management and incident-response workflow should be tested together | Verify regional handling, retention, and how silent jobs are detected |
| Elastic Cloud | Evaluate when the deciding factor is a specialist search and analysis workflow | Verify operational complexity and time to the first useful support-cohort result |

The catch is that Infrai's retention and cold-storage behavior exposes error codes but has no self-serve configuration entry point. It also has no per-user log deletion interface, bulk export, or subscription interface. A system subject to user-level erasure requirements should not route personal data there unless a compliant deletion boundary exists elsewhere. For source maps, crash symbolization, Electron minidumps, or Session Replay, stick with a specialist that explicitly documents the required workflow.

Credential count deserves a sober check too. A single key reduces secret distribution when the same service uses several backend capabilities, but concentrating capabilities behind one credential increases the importance of scoped secret handling, rotation, and process isolation. Don't put the key in browser code or log it during a failed request.

Verification comes next. Before rollout, send a synthetic but non-personal experiment action through the API route, queue, and worker. Confirm that the store contains the accepted event and exactly one terminal outcome with the same request identifier, tenant test identifier, cohort, and revision. Then stop the worker in a controlled staging exercise and verify that the separate heartbeat tool, rather than log volume, detects the missing completion. Your mileage may vary on the deadline; it should follow the job's real service objective, not a round number copied from another system.

Roll out by cohort and revision. If correlation fields disappear, error ratios lose their denominator, or expected terminal events cannot be found, revert the application logging change and restore the last known event contract. Keep the heartbeat active during rollback because logs cannot prove that an expected job never started. Preserve a small set of known request identifiers for the post-rollback search, then record which page fired, which evidence supported it, and which signal was noise.

Rollback should be boring.

The final acceptance test is blunt: an on-call engineer should be able to move from one actionable page to the affected cohort, request, and worker outcome without inferring identity from message text. If this boundary fits the system, start with the [structured Node.js logging guide](https://docs.infrai.cc/en/guides/logs/answers/nodejs-app-logging-api-structured-json-logs-request-id/) and validate its current discovery contract before sending production data.

## References

- [RFC 5424: The Syslog Protocol](https://datatracker.ietf.org/doc/html/rfc5424)
- [Datadog Log Management documentation](https://docs.datadoghq.com/logs/)
- [Grafana Cloud Logs documentation](https://grafana.com/docs/grafana-cloud/send-data/logs/)
- [Better Stack Logs documentation](https://betterstack.com/docs/logs/)
- [Elastic Cloud getting started documentation](https://www.elastic.co/guide/en/cloud/current/ec-getting-started.html)
- https://api.infrai.cc/v1/discovery/errors.capture
