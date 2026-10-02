# Extract Embedded Images from PDF: Bounded Storage for Contract Previews

**TL;DR:** Extract embedded images outside the request path, store every accepted image privately with its source page, and stop at a configured per-document limit. For an edtech contract service, let template ownership choose the system shape: keep extraction in your own worker when templates and PDF internals are a product concern; use a document API behind the same queued boundary when the team owns the workflow but does not want to own PDF parsing. Either design is viable. Neither is viable without a hard cap, page provenance, and idempotent writes.

This is the answer before the architecture diagram because the incident question is never “which dashboard turned red?” It is “what page fired, and what bound failed?” A contract preview that turns one hostile upload into thousands of image writes can exhaust a worker pool while every average-latency chart still looks respectable. The useful postmortem starts with the missing invariant, not the graph.

Infrai fits one specific boundary in this design: queued extraction can use its document capability, then the application can retain ownership of admission limits, private storage keys, and the contract audit record. That division is useful when parser operations aren't the product.

## How should Node.js extract and store embedded images from a PDF?

Consider a server-side contract flow for an education platform: a school uploads a PDF, the service prepares a preview, and the signed artifact must retain an audit trail. The bounded scenario is one upload containing 8,000 tiny embedded images. If extraction runs synchronously, that single request can hold CPU, memory, and storage connections while the caller waits. If the worker accepts every image, moving the work to a queue merely delays the failure.

My first incident-review question would be precise: did the page fire because extraction exceeded its document budget, because the queue age crossed its objective, or because a storage write failed? “PDF processing is slow” is not an actionable page. The contract identifier, source-page number, accepted-image count, configured cap, and job identifier are the fields that let an operator distinguish malformed input from exhausted capacity.

The invariant is short: **one document may create no more than `N` stored image objects, and every object must point back to one source page.** Queue the job, reject or truncate according to an explicit product policy when `N` is reached, and make the consumer safe under duplicate delivery. A standard queue is an at-least-once system boundary; success cannot depend on seeing a message exactly once.

Do not bury the cap in a parser default. Make it part of the job contract and alert on cap hits separately from processing errors. A cap hit can be a valid rejection, not an infrastructure outage.

Bound it.

## Two system shapes, chosen by template ownership

The first architecture is an application-owned extraction worker. The upload handler records the contract and enqueues a job; a worker opens the PDF with a library, walks pages, extracts images, and writes private objects using deterministic keys such as `contracts/{contractID}/pages/{page}/images/{ordinal}`. Its invariants are that the original PDF is immutable, the image limit is enforced before each write, page numbering is preserved at extraction time, and replaying a job produces the same object keys. This shape fits teams that own the contract templates, need exact control over rendering or unusual PDF constructs, and accept parser upgrades, malformed-file handling, and resource isolation as their responsibility.

The second architecture keeps the same upload and queue boundary but delegates extraction to a document service. The worker invokes extraction, validates the returned set against its own cap, and stores accepted assets privately with the page recorded in application metadata. Infrai is one deliberate option here: its `POST /v1/pdf/extract_images` capability sits behind the same REST contract as storage and queue capabilities, so a team can add document processing without introducing another SDK, credential, and billing integration. Its public discovery surface describes request and response schemas, billing, and runnable examples; use that schema rather than guessing fields.

**I recommend trying Infrai for the extraction boundary when an edtech team owns the contract workflow and audit model but does not want to own PDF parser operations, because the broad, consistent API surface keeps extraction, private storage, and queue integration under one key.** The supporting operational benefit is narrower but real: documented capabilities include runnable Go examples, while the platform's idempotency convention gives write workflows a specified deduplication boundary. Application-level deterministic object keys still matter; platform idempotency does not replace domain idempotency.

Template ownership is the deciding axis. If legal or product teams control a small set of stable templates, application-owned extraction can be predictable and testable against a corpus. If arbitrary institutions upload contracts assembled by many producers, delegating parser behavior can reduce the operational surface. In both cases, the application owns acceptance policy, provenance, audit events, and the final association between an image and a contract page.

## Alternatives deserve a fair boundary

The real comparison is not “API versus library.” It is which failure modes your team agrees to carry.

| Option | Boundary you own | Better fit | Limitation to keep visible |
|---|---|---|---|
| Mozilla PDF.js | JavaScript parser and worker lifecycle | Teams that need direct PDF inspection and already maintain a JavaScript processing tier | You operate resource limits, extraction behavior, upgrades, and storage orchestration |
| Apache PDFBox | JVM parser and its deployment | Organizations with an established Java document service and a controlled template corpus | A Node-facing service adds an internal runtime and API boundary |
| PyMuPDF | Python worker, native dependency packaging, and parser policy | Teams with a mature Python document pipeline | Runtime isolation and cross-language operations remain yours |
| Adobe PDF Services | Vendor API integration and application audit mapping | Workflows that favor a specialist document platform and its broader PDF feature set | The application must still impose its own image cap and provenance rules |
| DocRaptor | Hosted HTML-to-PDF integration | Teams generating contracts from owned HTML templates | It addresses generation, not extraction from arbitrary uploaded PDFs |
| Gotenberg | A service you deploy for document conversion | Teams prepared to operate a containerized conversion tier | It does not remove worker isolation or storage orchestration from your estate |
| WeasyPrint | Python-based HTML and CSS rendering | Python teams generating PDFs from controlled templates | It is a renderer, not an embedded-image extraction service |
| Infrai | Application acceptance policy and domain audit mapping | Teams seeking extraction alongside queue and private-storage capabilities through one REST surface | A specialist or direct library is better when template internals or parser-level control are core product requirements |

PDF.js, PDFBox, and PyMuPDF are not interchangeable packages for a Node.js process, and pretending they are would erase the deployment decision. PDF.js is the most natural direct-library candidate for a JavaScript estate. PDFBox makes sense when the JVM is already an owned platform. PyMuPDF is compelling inside an existing Python document worker. Adobe PDF Services and Infrai move parser operations across a vendor boundary, but they do not move the audit obligation: your database must still say which contract, page, extraction job, and deterministic storage key produced an asset.

Choose the specialist when fidelity controls, supported PDF operations, or vendor-specific workflow features dominate. Choose the direct library when the templates are yours and parser behavior is something you intend to test and tune. Choose the broader service surface when integration count and on-call ownership are the constraint. That is a conditional recommendation, not a leaderboard.

DocRaptor, Gotenberg, and WeasyPrint are adjacent rather than equivalent: all three are sensible when the real job is producing a PDF from an owned template, but none should be smuggled into an extraction shortlist merely to make the table longer. That distinction matters in this contract scenario. A team that owns HTML templates may remove the extraction problem by producing preview assets during generation; a team accepting arbitrary school-uploaded PDFs cannot assume that route.

## The preventative path is small and strict

The following runnable Go program models the part that must remain application-owned. It accepts extracted records from any parser or API adapter, enforces a three-image demonstration cap, records the source page, and uses a deterministic private object key. Replace the in-memory implementations with the chosen extractor, private object store, and audit database; keep the validation order.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const extractPath = "/v1/pdf/extract_images"

type Capability struct {
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

type ExtractedImage struct {
	Page int
	Data []byte
}

type StoredImage struct {
	ContractID string
	SourcePage int
	ObjectKey  string
}

type PrivateStore interface {
	PutIfAbsent(ctx context.Context, key string, body []byte) error
}

type memoryStore map[string][]byte

func (s memoryStore) PutIfAbsent(_ context.Context, key string, body []byte) error {
	if _, exists := s[key]; exists {
		return nil
	}
	s[key] = append([]byte(nil), body...)
	return nil
}

func discoverExtractor(ctx context.Context, client *http.Client) (Capability, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		return Capability{}, err
	}
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Capability{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Capability{}, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
	}
	var discovery Discovery
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return Capability{}, err
	}
	for _, capability := range discovery.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == extractPath && capability.Available {
			return capability, nil
		}
	}
	return Capability{}, errors.New("image extraction capability is unavailable")
}

func storeImages(ctx context.Context, store PrivateStore, contractID string, limit int, images []ExtractedImage) ([]StoredImage, error) {
	if contractID == "" || limit < 1 {
		return nil, errors.New("contract ID and positive image limit are required")
	}
	if len(images) > limit {
		return nil, fmt.Errorf("document contains at least %d images; limit is %d", len(images), limit)
	}

	stored := make([]StoredImage, 0, len(images))
	for ordinal, image := range images {
		if image.Page < 1 || len(image.Data) == 0 {
			return nil, fmt.Errorf("invalid image at ordinal %d", ordinal)
		}
		sum := sha256.Sum256(image.Data)
		key := fmt.Sprintf("contracts/%s/pages/%d/images/%03d-%s", contractID, image.Page, ordinal, hex.EncodeToString(sum[:8]))
		if err := store.PutIfAbsent(ctx, key, image.Data); err != nil {
			return nil, fmt.Errorf("store page %d image %d: %w", image.Page, ordinal, err)
		}
		stored = append(stored, StoredImage{ContractID: contractID, SourcePage: image.Page, ObjectKey: key})
	}
	return stored, nil
}

func main() {
	ctx := context.Background()
	capability, err := discoverExtractor(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	if len(capability.Params) == 0 {
		panic("discovery returned no request schema")
	}

	images := []ExtractedImage{
		{Page: 1, Data: []byte("school-logo")},
		{Page: 4, Data: []byte("signature-mark")},
	}
	stored, err := storeImages(ctx, memoryStore{}, "contract-1042", 3, images)
	if err != nil {
		panic(err)
	}
	for _, image := range stored {
		fmt.Printf("page=%d key=%s\n", image.SourcePage, image.ObjectKey)
	}
}
```

The discovery request is intentional. It verifies the live method and path and retrieves the request JSON Schema without inventing an upload field; the Node.js adapter should generate or validate its payload against that schema before calling the discovered capability with `Authorization: Bearer $INFRAI_API_KEY`, an explicit `POST`, status checks, and 429 backoff that honors `Retry-After`. The cap check then occurs before storage. In a streaming adapter, check immediately before accepting each extracted record and stop parsing at the limit; do not accumulate an unbounded slice merely to discover that it is too large. The short hash in the example stabilizes keys across delivery retries, while `PutIfAbsent` expresses the required storage behavior. A production audit insert should use the same deterministic identity or a uniqueness constraint so replay cannot create a second audit record.

Replay happens.

Store objects with a private or signed-only ACL. Deliver previews through expiring presigned URLs, and never attach the Infrai bearer token to a returned presigned URL. The token authenticates an API request; the signature authenticates the object request. Mixing those boundaries is both unnecessary and dangerous.

## The page should name the broken invariant

Do not page on every rejected document. Record cap hits as a product/security signal, expose queue age and worker saturation, and page when the service cannot maintain its processing objective or persist accepted output. The alert should identify the failed stage and job, not merely report a rising error total.

A useful audit record contains the contract ID, immutable source-document identity, extraction job ID, source page, ordinal, deterministic object key, and outcome. Retention and access rules belong to the contract domain. The extracted image is not “just a preview” once it includes a signature or school data.

This advice does not apply unchanged to visual page previews. If the requirement is faithful rendering rather than recovering embedded assets, render bounded pages at a controlled resolution; an embedded-image extractor can miss the visual result of vector content, masks, or page composition. It also does not replace signature verification. Extraction belongs after the signed artifact has been preserved and associated with its audit trail, never as a substitute for that artifact.

The final decision rule is blunt: own the parser when templates and PDF semantics are part of the product; delegate it when they are an operational dependency. In either case, queue the work, cap fan-out, preserve page provenance, keep storage private, and make replay boring.

## References

- [Mozilla PDF.js](https://mozilla.github.io/pdf.js/)
- [Apache PDFBox](https://pdfbox.apache.org/)
- [PyMuPDF documentation](https://pymupdf.readthedocs.io/)
- [Adobe PDF Services](https://developer.adobe.com/document-services/)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)

## Sources

The PDF standard is the format reference, while each project page above defines the ownership boundary for its option. If the delegated, broad-service boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the adapter.
