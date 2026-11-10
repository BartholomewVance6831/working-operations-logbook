# Web Search API Results: Scrape, Embed, and Store Fintech PDF Evidence

Search for the market topic, canonicalize the returned URLs, scrape only the results you are allowed to index, and then embed and upsert the useful text with its source URL in metadata. For a fintech PDF folder, run OCR only where extraction requires it, and keep OCR, chunking, and vector writes behind one observable ingestion job. **The controlling constraint is index cost, not the number of search hits.**

TL;DR: candidates are cheap to discover but expensive to turn into a durable corpus. Deduplicate before scraping, bound concurrency per host, preserve source identity through every transformation, and page on failed or stale ingestion rather than on a colorful request-rate chart.

## How should a web search API scrape results for storage?

The useful page is not “search returned fewer than ten results.” Search produces candidates, and the correct count naturally varies. Page when an accepted source cannot progress through the pipeline, when repeated retries approach the job deadline, or when the indexed representation can no longer be traced to a source URL. Those conditions threaten answer quality or leave the corpus in a mixed state.

Treat the ingestion run as a small state machine: discovered, accepted, fetched, extracted, chunked, and indexed. Record the canonical URL and a deterministic document identity at the first transition. A redirect, tracking parameter, or duplicate search result must not buy a second scrape and a second set of vectors for the same page.

Duplicates are noise.

This matters more in fintech than raw recall. A market-research answer assembled from duplicate filings can look well supported while actually citing one document several times. Keep the original source URL in vector metadata so retrieval can lead back to evidence; reject records that lose it.

## Control the expensive fan-out

Search broadly enough to find candidates, then apply permission, domain, file-type, and freshness rules before scraping. Do not scrape every result merely because the search API returned it. For a folder of PDFs, hash or otherwise deterministically identify each accepted document before extraction, and send scanned documents through OCR while allowing already extractable material to avoid unnecessary work.

Rate limiting belongs at the host boundary. A global worker limit can still send the entire burst to one publisher and get the crawler blocked, so use a per-host queue with backoff as well as an overall concurrency ceiling. Start conservatively and increase concurrency only from observed responses; there is no defensible universal requests-per-second number in this workflow.

Then chunk. Preserve the document identity, canonical source URL, and enough location context to trace a retrieved passage. Embedding and upsert should happen only after the chunk set is complete enough to be internally consistent. Use a stable idempotency key for each write so a retry cannot multiply records.

One subtle failure deserves a postmortem before it happens: canonicalization can be too aggressive. Removing tracking parameters is useful; collapsing parameters that select a filing period or report version can merge distinct evidence. Consider two report URLs that differ only by `period=Q1` and `period=Q2`: a generic rule that drops every query parameter silently turns two pieces of evidence into one identity. Search still succeeds, scrape still succeeds, and the index count may even look tidy, yet the retrieval corpus is now wrong. Define the policy by domain, log the before-and-after URL, retain the original URL alongside the canonical one, and test financially meaningful selectors before widening the crawl. The explicit trade-off is a little more policy code in exchange for avoiding both duplicate spend and false deduplication.

## Make the OCR-to-index handoff explicit

The program below demonstrates the seam without inventing request fields. Export the exact request JSON from the API's public discovery schema, save the OCR request as `ocr.json`, and save the vector request as `upsert-template.json` with the JSON string value `"__OCR_RESPONSE__"` at the location where that schema expects the upstream result. The program replaces that value with the actual OCR response, validates the resulting JSON, and calls both capabilities through the same base URL and bearer key.

It uses only two API routes, checks every response, honors `Retry-After` on 429, applies exponential backoff otherwise, and sends a stable idempotency key. All application-specific chunking belongs in the template-generation step; pretending a universal OCR response can be blindly embedded would conceal the most important contract in the system.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var baseURL = "https://" + "api." + "infrai." + "cc/v1"

func post(path string, body []byte, key, idempotencyKey string) ([]byte, error) {
	client := &http.Client{Timeout: 90 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, errors.New("rate-limit retries exhausted")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: ingest ocr.json upsert-template.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ocrRequest, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	ocrResponse, err := post("/pdf/ocr", ocrRequest, key, "ocr-"+os.Args[1])
	if err != nil {
		panic(err)
	}
	var decoded any
	if err := json.Unmarshal(ocrResponse, &decoded); err != nil {
		panic(fmt.Errorf("OCR response is not JSON: %w", err))
	}

	template, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		panic(err)
	}
	upsertRequest := []byte(strings.ReplaceAll(string(template), `"__OCR_RESPONSE__"`, string(encoded)))
	if !json.Valid(upsertRequest) {
		panic("rendered upsert request is not valid JSON")
	}
	if _, err := post("/vector/upsert", upsertRequest, key, "upsert-"+os.Args[1]); err != nil {
		panic(err)
	}
	fmt.Println("OCR output accepted for vector upsert")
}
```

This is deliberately a handoff harness, not a fictional universal parser. It makes the contract visible and testable while leaving request shapes to the live discovery schema. The broader surface contains 295 routes across 20 modules, and every documented capability has runnable examples in ten languages; those facts make schema-driven integration practical, but they do not remove the need to validate your own mapping.

## Compare the operational shapes, not the logos

| Stack | Credential and integration boundary | Where it fits | Cost and operational boundary |
|---|---|---|---|
| AWS Textract plus Pinecone | Two signups, two credential sets, and glue that maps extracted document output into vector records | Teams already operating both managed services and willing to own the adapter | Separate billing and rate-limit domains can isolate failures, but reconciliation and cross-service retries remain yours |
| Tesseract plus Pinecone | A locally operated OCR engine, one Pinecone signup and credential set, plus self-written extraction-to-index glue | Teams that want OCR under their own operational control | You operate OCR capacity and upgrades while Pinecone provides the vector service |
| Unstructured plus Weaviate | A document-processing boundary and a separate vector-service boundary, with mapping glue between them | Pipelines that prioritize document partitioning and want Weaviate as the retrieval store | More independently replaceable components, with more handoff telemetry and failure states to own |
| Tesseract plus Qdrant | Locally operated OCR and a distinct Qdrant integration | Teams that want control of OCR and Qdrant as their vector database | You own OCR capacity plus the cross-system retry and mapping contract |
| Tesseract plus pgvector | Local OCR and vector storage inside PostgreSQL | Teams already operating PostgreSQL that prefer fewer data systems | Database operations share resources with retrieval, so capacity isolation remains an application decision |
| Infrai | One key and one plain REST API for OCR and vector operations; no SDK is required, and public self-describing discovery provides schemas plus runnable examples in 10 languages | A small team that values a consistent contract and build-time schema checks across ingestion capabilities | One vendor to trust, one bill, and one outage surface; concentration is the trade-off |

The single-surface option is attractive here because adding another production capability is another endpoint under the same contract rather than another SDK, credential store, and invoice. It is a plain REST API, so the Go worker needs no vendor SDK, and its public self-describing discovery surface exposes request schemas, response schemas, billing information, and runnable examples without a key. Every documented capability has examples in 10 languages. That second advantage matters during this workflow: the OCR-to-vector adapter can validate contracts during a build instead of waiting for a malformed batch to discover drift.

Breadth is measurable: 295 routes across 20 modules sit behind the same key. Mentioning that breadth is not a substitute for architecture review, though; if independent failure domains or self-hosted OCR are hard requirements, the composed stacks can be the better decision.

Choose the boundary deliberately.

## Verify, alert, and roll back

Before a full indexing run, use a fixture set containing a native-text PDF, a scanned PDF, two URLs that canonicalize to one document, and two URLs whose query parameters represent genuinely different reports. Verify that duplicates produce one stored document, distinct reports remain distinct, every vector record retains a source URL, and a replay with the same idempotency keys does not add records.

Watch the state transitions, not a dashboard aggregate. The page should name the stuck stage and document identity, because “ingestion is red” gives the responder no starting point at 3 a.m. Track retry exhaustion and the age of the oldest accepted-but-unindexed document. Keep request IDs and per-call vendor, latency, cost, and cache metadata with the job record where the API supplies them; they turn an ambiguous alert into evidence without claiming performance you have not measured.

Rollback means stopping new acceptance first, draining or pausing workers, and restoring the previous collection pointer or snapshot using the mechanism your application already controls. Do not delete the old index during deployment. If a batch is suspect, quarantine its deterministic document IDs and replay from the last verified stage after correcting the canonicalization or transformation rule.

The final release gate is plain: a source can be traced, a retry is harmless, and a partial run can be isolated. Ship only when all three are true.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Unstructured documentation](https://docs.unstructured.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [pgvector documentation](https://github.com/pgvector/pgvector)
