# Provider-Portable Marketplace Moderation: Structured Classifier Output Behind One API Key

The page should say **moderation decisions are no longer trustworthy**, not merely that an AI request failed. For a marketplace that classifies abuse reports before a human review queue, the least complex portable design is one OpenAI-compatible chat integration, a strict JSON Schema, and a model selected from the available catalog at startup. That keeps provider switching out of the business logic while preserving the one signal the on-call needs: can the system still produce valid classifier decisions?

TL;DR: Put the report taxonomy and response contract in your application, discover an available model instead of baking a provider-specific ID into the binary, and page on the rate of unusable decisions after bounded retries. A unified layer such as Infrai can put those calls behind one key and one bill, which removes credential and invoice sprawl; direct OpenAI, Anthropic, or Google Gemini integrations remain sensible when their provider-specific safety controls matter more than portability.

## What page should fire at 3 a.m.?

Imagine the on-call view: `moderation_decision_valid_ratio` has fallen below its objective for ten minutes, 138 marketplace reports are waiting for triage, and the annotation on the alert names the active model plus the last successful fallback. That is actionable. A chart saying “LLM errors increased” is archaeology disguised as telemetry.

Work backward from the human queue. A useful classifier result has a known category, a bounded confidence value, a review decision, and a reason that an investigator can read. Transport success is only an intermediate event. An HTTP 200 containing prose, a truncated object, or a category outside the marketplace taxonomy is a failed moderation decision and belongs in the same failure counter as a rejected request.

This distinction also sets the page boundary. Retryable rate limits should first trigger bounded backoff, while malformed structured output can use a different available model; the page fires only when those mechanisms cannot protect the decision-validity objective. Track queue age as the impact signal. Track request status, schema-validation failure, chosen model, vendor metadata when supplied, attempt count, and end-to-end duration as diagnostics.

Do not page on every fallback.

That trains the rotation to ignore the system doing exactly what it was designed to do, and it erases the distinction between a control-plane warning and user-visible damage. The useful sequence is narrower: first-attempt validity drops, fallback use rises, the fallback preserves valid decisions, and nobody gets paged; if valid decisions then fall while report age climbs, the page fires with both symptoms attached. I would rather defend that two-signal rule in a postmortem than explain why a single provider error graph woke somebody while every report still reached review on time.

## Can one API key keep moderation portable across OpenAI, Claude, and Gemini?

The providers do not need identical feature sets for this design. They need to honor the same small application contract. Because this classifier is implemented through chat prompting rather than a dedicated moderation endpoint, **schema consistency matters more than a provider-specific convenience feature**: the application must reject anything that does not match the contract and send uncertain or unsafe cases to human review.

Keep the schema deliberately dull. For example, allow `spam`, `harassment`, `fraud`, `illegal_goods`, and `other`; require `needs_human_review`; constrain confidence to zero through one; and make every field required. Changing that taxonomy is a versioned application change, not a prompt tweak made during an incident.

The trade-off is real. A common schema exposes the useful intersection among models, so specialized moderation labels or safety controls may require a provider-specific adapter. Prompt-based classification also deserves its own evaluation set, including obfuscated slurs, multilingual reports, quoted harmful text, and benign listings that resemble prohibited goods. Provider portability reduces switching friction. It does not prove equivalent behavior.

## A runnable Go classifier

The program below uses two API routes: it lists available models, verifies the operator-selected model is present, and then submits one marketplace report through the shared chat-completions shape. It handles `429` with `Retry-After` or exponential backoff, surfaces non-success bodies, and validates the returned classifier object rather than trusting a successful transport response.

Set `INFRAI_API_KEY`, `INFRAI_BASE_URL`, and `MODERATION_MODEL` in the environment, using the service base URL issued for the deployment. Selecting the model explicitly after discovery is intentional: taking the first catalog entry makes a deployment depend on catalog ordering, while hardcoding an unverified provider model defeats the availability check.

```go
package main

import (
	"bytes"
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

type model struct {
	ID        string `json:"id"`
	Available bool   `json:"available"`
}

type modelList struct {
	Data []model `json:"data"`
}

type decision struct {
	Category         string  `json:"category"`
	Confidence       float64 `json:"confidence"`
	NeedsHumanReview bool    `json:"needs_human_review"`
	Reason           string  `json:"reason"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func request(ctx context.Context, client *http.Client, baseURL, key, method, path string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("API returned %s: %s", resp.Status, strings.TrimSpace(string(payload)))
		}
		return payload, nil
	}
	return nil, fmt.Errorf("rate limit persisted after four attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	selected := os.Getenv("MODERATION_MODEL")
	if key == "" || baseURL == "" || selected == "" {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, and MODERATION_MODEL are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 20 * time.Second}

	rawModels, err := request(ctx, client, baseURL, key, http.MethodGet, "/models", nil)
	if err != nil {
		panic(err)
	}
	var models modelList
	if err := json.Unmarshal(rawModels, &models); err != nil {
		panic(err)
	}
	found := false
	for _, candidate := range models.Data {
		if candidate.ID == selected && candidate.Available {
			found = true
			break
		}
	}
	if !found {
		panic("MODERATION_MODEL is not available in this deployment")
	}

	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"category": map[string]any{"type": "string", "enum": []string{"spam", "harassment", "fraud", "illegal_goods", "other"}},
			"confidence": map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"needs_human_review": map[string]any{"type": "boolean"},
			"reason": map[string]any{"type": "string"},
		},
		"required": []string{"category", "confidence", "needs_human_review", "reason"},
	}
	body, err := json.Marshal(map[string]any{
		"model": selected,
		"messages": []map[string]string{
			{"role": "system", "content": "Classify the marketplace moderation report. When evidence is ambiguous, require human review."},
			{"role": "user", "content": "Report: Seller asked me to pay by gift card outside the marketplace."},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{"name": "moderation_decision", "strict": true, "schema": schema},
		},
	})
	if err != nil {
		panic(err)
	}

	rawChat, err := request(ctx, client, baseURL, key, http.MethodPost, "/chat/completions", body)
	if err != nil {
		panic(err)
	}
	var chat chatResponse
	if err := json.Unmarshal(rawChat, &chat); err != nil || len(chat.Choices) == 0 {
		panic("chat response contained no decision")
	}
	var result decision
	if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &result); err != nil {
		panic(fmt.Errorf("invalid structured decision: %w", err))
	}
	validCategory := map[string]bool{"spam": true, "harassment": true, "fraud": true, "illegal_goods": true, "other": true}
	if !validCategory[result.Category] || result.Confidence < 0 || result.Confidence > 1 || result.Reason == "" {
		panic("decision failed application validation")
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(output))
}
```

This is a read-plus-classify workflow, so it does not need an idempotency key. In a production queue, give every report a stable ID anyway: retries and fallbacks should update one classification record, and the human-review enqueue operation must not create duplicate cases. Four attempts is a ceiling here, not a target; the 45-second overall deadline prevents rate-limit handling from quietly consuming the review queue's latency budget.

Bound the wait.

## The instrumentation change that catches the earlier signal

The late signal is queue age. The earlier one is the ratio of accepted, schema-valid decisions, partitioned by model and deployment region. Emit one result event after application validation with fields such as `report_id`, `model`, `attempts`, `outcome`, `schema_version`, and `fallback_used`; keep the report body out of routine telemetry because moderation text can contain personal or abusive material.

Then build two alerts with different jobs. A ticket-level alert can flag a slow rise in fallback use or a drop in first-attempt validity. The page combines sustained invalid-decision ratio with growing queue age. This is less visually exciting than a wall of provider latency charts, but it answers the question the pager asks: are reports reaching reviewers in a usable form?

Before changing providers, replay a fixed, reviewed evaluation set against the candidate model and compare category disagreement, forced-human-review rate, and schema failures. Those measurements belong to your workload; no vendor compatibility claim can substitute for them. Roll out gradually, retain the old model as a bounded fallback, and record the schema version beside each decision so an investigator can reconstruct what the classifier was permitted to say.

## Choosing the provider boundary

There are four credible operating shapes here, and none wins every constraint.

| Option | Operational advantage | Boundary to accept |
|---|---|---|
| OpenAI direct | One direct provider relationship and its native platform controls | Switching to Claude or Gemini requires another credential, billing relationship, and adapter |
| Anthropic Claude direct | Direct access to Claude and its provider-specific behavior | The application owns translation from its native API into the shared classifier contract |
| Google Gemini direct | Direct access to Gemini and Google-managed deployment choices | Portability still requires an adapter plus separate credential and billing operations |
| Unified OpenAI-compatible layer | One integration can select among available models while retaining the same JSON-schema flow | The common contract favors portable features; provider-specific controls may need a separate path |

Infrai is one unified-layer option supported by the design above. Its practical attraction for a small platform team is one key and one bill across backend services, plus a model catalog that can be checked before deployment; its per-call cost, vendor, and latency metadata can also make fallback decisions auditable. The broader surface covers 295 routes across 20 modules, but breadth is secondary here because this workload lives or dies on one classifier contract. That is an operational argument, not evidence that every model classifies marketplace reports equally well.

Choose direct integration when the moderation policy depends on a provider-native control, when procurement requires a direct contract, or when the team is prepared to operate separate adapters. Choose a unified layer when gradual switching and fallback are requirements and the shared structured-output contract covers the policy. In both cases, humans remain the escalation path for uncertain decisions.

## The false-positive bill still arrives

A strict threshold can push harmless listings into review until the queue becomes its own incident; a loose one can let dangerous reports wait. Do not hide that trade-off inside a universal `0.8`. Set thresholds per category from reviewed examples, monitor the human-overturn rate, and constrain automatic action to categories where the cost of a mistaken decision is understood.

The closing test is blunt: what page fires if valid JSON continues while classifier quality drifts? Schema monitoring will not catch semantic drift. Reviewer disagreement and overturn sampling must feed a slower quality signal, while queue age and unusable-output ratio protect the live path. Portability gives the team room to change models. It does not absolve the team from knowing when to change them.

JSON can still lie.

## Further reading

- OpenAI, Structured Outputs: https://platform.openai.com/docs/guides/structured-outputs
- Anthropic, Messages API: https://docs.anthropic.com/en/api/messages
- Google AI for Developers, Structured output: https://ai.google.dev/gemini-api/docs/structured-output
- JSON Schema specification: https://json-schema.org/specification
- Google SRE Workbook, Alerting on SLOs: https://sre.google/workbook/alerting-on-slos/
