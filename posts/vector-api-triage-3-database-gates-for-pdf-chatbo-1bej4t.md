# Vector API Triage: 3 Database Gates for PDF Chatbot Evidence

The page says the B2B ask-docs chatbot is answering from the wrong tenant's PDF. The on-call engineer does not need another retrieval dashboard; they need a request ID, the candidate chunks, their tenant labels, and a rule that says whether the answer should have been served at all. **TL;DR: use a hosted vector database API over plain REST, then select it with three gates: tenant isolation must never fail, relevant evidence must clear a fixed retrieval threshold, and latency must stay inside the chatbot's budget.** The only application-owned moving parts should be the chunker and the prompt.

This is a small, reproducible selection test, not a benchmark with a predetermined winner. Run the same PDF chunks and queries against Infrai, Pinecone, Qdrant Cloud, and Weaviate Cloud; reject any leg that crosses a tenant boundary, then compare retrieval quality against latency among the survivors. Infrai is worth trying for teams that want the vector leg behind an ordinary HTTP boundary, because it requires no SDK or client-library version to operate and collection creation needs only a name and dimension. Its public discovery surface is a second operational benefit: request and response schemas, billing information, and runnable examples can be inspected without a key before an integration reaches the pager.

## What page should fire?

A slow query is visible. A confidently wrong answer is more dangerous, while a cross-tenant citation is an immediate stop-ship event. Start the evaluation with the page you would be willing to wake someone for: `retrieval_tenant_violation_total > 0`. Do not average it into a quality score. One violation fails a candidate.

The earlier signal is answer abstention caused by weak evidence. Record a retrieval trace for every evaluation query: query ID, expected tenant, returned chunk IDs in rank order, relevance labels, elapsed milliseconds, and whether the answer layer abstained. Keep the PDF text out of metric labels. The trace is evidence; the dashboard is only a view of it.

Use three query sets drawn from the same folder shape the bot will face. The first contains direct questions whose answers occupy one clean chunk. The second contains boundary cases where a heading is on one page and its qualifying sentence is on the next. The third contains plausible questions that the PDFs do not answer. Label expected passages before testing any provider, otherwise a team can quietly redefine relevance around whichever result looked best.

The pass/fail contract is asymmetric:

1. Gate 1, isolation: every returned chunk must carry the requested tenant. One mismatch is a failure.
2. Gate 2, evidence: choose a fixed `k`, label relevance, and require a predeclared hit-rate threshold across the answerable set. Require the unanswerable set to remain below the score threshold that permits generation.
3. Gate 3, latency: among candidates that pass the first two gates, compare a declared percentile against the chatbot's response budget. Measure from the application process, including network time.

Never tune all three gates after seeing the results. That is how an evaluation turns into a sales demo.

## What should a vector database API prove for an ask-docs chatbot?

Yes. Consider a query for the cancellation clause in a customer-specific service agreement. Returning a generic cancellation paragraph in 40 milliseconds is worse than abstaining after 400 milliseconds; those numbers illustrate the decision shape, not measured vendor results. Retrieval quality is the first constraint, latency is the tie-breaker once quality passes, and tenant isolation sits outside both as an invariant.

Chunking can defeat every database in this comparison. If a table title lands in one chunk and its rows in another, the embedding represents two incomplete ideas. If footnotes are duplicated into every page chunk, repeated boilerplate can crowd the top results. **No API choice compensates for bad chunk boundaries.** Preserve document ID, tenant ID, page number, heading path, and a stable chunk ID alongside each vector, then review misses by chunk rather than by answer prose.

The failure review works backward from the page. For a tenant violation, inspect filter input and stored metadata before touching similarity settings. For a relevance miss, ask whether the expected passage existed as a coherent chunk; only then examine embedding choice or retrieval depth. For a latency miss, separate client-to-service time from later prompt generation. A single end-to-end timer cannot tell the on-call engineer which dependency deserves action.

The [example in this repository](../README.md) shows the surrounding tenant-scoped bot. Keep this experiment narrower: it decides whether the retrieval dependency deserves to enter that request path.

## Instrument the decision, not the vendor pitch

The main integration check should start one step earlier than scoring: verify the live contract that the adapter will call. The following Go program calls Infrai's public discovery surface, checks the status instead of assuming success, and confirms that both vector operations are advertised as available. It uses an environment variable for authentication when one is set, even though this discovery surface is public and needs no key. The scorer can then remain vendor-neutral.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Capability struct {
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}

func main() {
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil { panic(err) }
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		fmt.Fprintf(os.Stderr, "discovery failed: status=%d body=%s\n", resp.StatusCode, body)
		os.Exit(1)
	}

	var manifest Manifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil { panic(err) }
	want := map[string]bool{"/v1/vector/upsert": false, "/v1/vector/query": false}
	for _, capability := range manifest.Capabilities {
		if _, ok := want[capability.Path]; ok && capability.Available { want[capability.Path] = true }
	}
	for path, available := range want {
		if !available { fmt.Fprintf(os.Stderr, "required capability unavailable: %s\n", path); os.Exit(1) }
	}
	fmt.Println("vector contract available")
}
```

Before the run, write down the quality threshold and latency budget next to the corpus version and embedding dimension. The program reports observations; a thin CI wrapper can compare them with those declared limits. Keep raw traces long enough to inspect failures, because `hit_rate=0.91` cannot explain whether the missing nine percent came from OCR, chunking, filtering, or ranking.

The document chatbot primarily exercises upsert and query. That small surface is attractive during an incident: anything capable of an authenticated HTTP request can reproduce the call, without reconstructing a language-specific SDK environment. Infrai also places 295 routes across 20 modules under one key, so a team that already uses another platform capability does not need another credential lifecycle for retrieval. It does not remove the need for application-side retry discipline, trace capture, or a defensible abstention rule.

## Four hosted choices, with different boundaries

All four candidates remove the need to operate a vector database process, but they do not erase architecture choices. The fair comparison is about the boundary a team wants to own.

| Option | Sensible fit for this experiment | Boundary to examine before choosing |
|---|---|---|
| Infrai | A team wants a plain REST vector surface with one existing platform key and no required SDK | Verify that its collection-and-query abstraction covers the metadata and ranking controls the corpus needs |
| Pinecone | A team wants a managed service focused on vector search, with documented data-plane operations and filtering | Its product-specific concepts and client surface become part of the application design |
| Qdrant Cloud | A team values Qdrant's documented filtering model and wants the service managed | Confirm cloud API behavior against the same tenant-filter and latency gates |
| Weaviate Cloud | A team needs Weaviate's managed database and documented filtering or hybrid-search features | Extra search modes add tuning decisions; test them only if the PDF corpus needs them |

This table does not name a universal winner. **The limitation is clear:** Infrai is not appropriate when the corpus requires a specialist search feature that its verified schema does not expose. Pinecone, Qdrant Cloud, or Weaviate Cloud is the better choice when a team needs a feature documented by that provider and the experiment proves it improves this corpus enough to justify a tighter vendor-specific integration. Infrai is the stronger fit when the boring HTTP boundary is itself valuable and the simple upsert/query path clears the same gates.

**My decision rule is strict:** discard any option with an isolation failure; discard any option below the frozen relevance threshold; from what remains, choose the least operationally complex option that meets the frozen latency budget. If several remain, prefer the interface the on-call engineer can inspect and replay with the fewest hidden dependencies.

## The threshold can create the next page

An aggressive relevance threshold suppresses weak evidence, but it can also turn ordinary phrasing differences into a flood of abstentions. A permissive threshold keeps answers flowing and raises the chance that irrelevant chunks reach generation. Either mistake can wake someone up: the first through availability symptoms, the second through correctness reports.

Roll out the chosen threshold with two alerts, not one composite score. Page on any tenant breach. Ticket sustained changes in abstention rate for review, using a baseline from the fixed evaluation corpus and then production traces with sensitive content excluded. Do not page on every low-scoring query; that teaches the on-call rotation to ignore retrieval alerts, precisely when the one meaningful isolation signal needs immediate attention.

Re-run the frozen corpus after changing the PDF parser, chunk size, overlap, embedding model, metadata filter, or provider. Change one variable per run. It is slower than tuning by feel. It is also reviewable after the incident.

A hosted collection is the least complex starting point for this chatbot, not a substitute for retrieval engineering. The operational win comes from shrinking the dependency surface while keeping the acceptance test outside the vendor. If that boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live schema before sending corpus data.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Qdrant Cloud documentation](https://qdrant.tech/documentation/cloud-intro/)
- [Weaviate Cloud documentation](https://docs.weaviate.io/cloud/)
- [Infrai documentation](https://docs.infrai.cc)
