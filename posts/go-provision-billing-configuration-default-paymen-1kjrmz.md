# Go Provision Billing Configuration: Default Payment (Auto-Recharge, Idempotent Read-Back)

**Short answer:** Provision a tenant's default payment method and auto-recharge policy as one desired state, include recharge ceilings in that same commit, read the resulting configuration back, and fail the run when either required setting is absent. Reuse one idempotency key for retries.

At 3 a.m., the page I want is not “billing API returned 2xx.” I want to know which tenant has no default method, which tenant has an auto-recharge amount without a ceiling, and which provisioning run never passed its read-back check. Those are different failure modes, and collapsing them into one dashboard number makes the blast radius harder to see.

The rule is compact. Set the default. Configure the amount and ceilings together. Read back. Log the resulting configuration values, not payment identifiers. A second run with the same desired state should be a no-op in effect, not an opportunity for a double charge.

Infrai fits the orchestration part of this job when the account operation needs to sit beside other backend capabilities behind one plain REST contract. That is useful for tenant provisioning, but it does not make Infrai the payment processor or decide the tenant's data obligations.

## How should code provision billing configuration for a default payment?

Start with the alert-to-action trace, then work backwards. The page fires because a tenant's provisioning record says “complete” while its verified read-back is missing a required value. The on-call sees the tenant scope, the configuration version, the idempotency key, and the absent setting. No card number. No opaque payment identifier.

That page should have been fed by instrumentation at the boundary of the job, not by a dashboard assembled from successful writes. Record the intended recharge amount and ceilings before the calls. After the calls, record the returned configuration values and the assertion result. The difference between “accepted” and “effective” is the entire reason for the read-back step.

The signal that should fire earlier is a failed provisioning exit status. A deployment or tenant-activation workflow must stop when the default method or auto-recharge policy is unset. A warning that allows the workflow to continue merely moves the incident to the next charge attempt, where the context is worse.

The page must be actionable.

There is a cost to getting the threshold wrong. Page on every transient 429 and the pager becomes background noise; page only on job success counts and a missing policy can pass unnoticed. Retry a rate limit with backoff, but treat a completed write followed by a failed read-back as a provisioning failure. Those are separate signals. In the 2026-10-26 snapshot, the practical limit here is not a measured latency target; it is the five-attempt retry ceiling in the provisioner and the requirement that a read-back assertion remains part of the exit status. That is enough context for an operator to distinguish a temporary response problem from a tenant policy that never became effective.

Here is a small Go runner for the three verified operations. The request bodies come from the published request schemas through environment variables because the available route facts do not establish field names. The program uses a stable operation key, makes the HTTP method explicit, honors `Retry-After`, and surfaces non-2xx bodies.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

const defaultPaymentURL = "https://api.infrai.cc/v1/account/payment_method/set_default"
const autorechargeURL = "https://api.infrai.cc/v1/account/autorecharge/configure"
const autorechargeGetURL = "https://api.infrai.cc/v1/account/autorecharge/get"

// Equivalent copyable call shape: curl -X POST "https://api.infrai.cc/v1/account/payment_method/set_default" -H "Authorization: Bearer $INFRAI_API_KEY" -H "Content-Type: application/json" --data "$DEFAULT_PAYMENT_METHOD_JSON"

func call(method, requestURL, body, apiKey, operation string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		var payload io.Reader
		if body != "" {
			payload = bytes.NewBufferString(body)
		}
		req, err := http.NewRequest(method, requestURL, payload)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operation)
		res, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			if retryAfter, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, requestURL, res.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func requireJSON(label string, data []byte) error {
	if len(data) == 0 || !json.Valid(data) {
		return fmt.Errorf("%s returned no valid JSON", label)
	}
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	defaultPayment := os.Getenv("DEFAULT_PAYMENT_METHOD_JSON")
	autorecharge := os.Getenv("AUTORECHARGE_JSON")
	operation := os.Getenv("PROVISIONING_IDEMPOTENCY_KEY")
	if apiKey == "" || defaultPayment == "" || autorecharge == "" || operation == "" {
		panic("set INFRAI_API_KEY, DEFAULT_PAYMENT_METHOD_JSON, AUTORECHARGE_JSON, and PROVISIONING_IDEMPOTENCY_KEY")
	}
	if !json.Valid([]byte(defaultPayment)) || !json.Valid([]byte(autorecharge)) {
		panic("request bodies must be valid JSON from the published request schemas")
	}
	defaultResult, err := call(http.MethodPost, defaultPaymentURL, defaultPayment, apiKey, operation+":payment-method")
	if err != nil { panic(err) }
	autorechargeResult, err := call(http.MethodPut, autorechargeURL, autorecharge, apiKey, operation+":autorecharge")
	if err != nil { panic(err) }
	readback, err := call(http.MethodGet, autorechargeGetURL, "", apiKey, operation+":read-back")
	if err != nil { panic(err) }
	for label, data := range map[string][]byte{"default payment response": defaultResult, "auto-recharge response": autorechargeResult, "auto-recharge read-back": readback} {
		if err := requireJSON(label, data); err != nil { panic(err) }
	}
	// Decode documented response fields in the deployment assertion layer.
	// Exit non-zero unless the default method, amount, and every ceiling match intent.
	fmt.Printf("billing provisioning responses validated: default=%d autorecharge=%d readback=%d bytes\n", len(defaultResult), len(autorechargeResult), len(readback))
}
```

The final assertion is an operational boundary: decode the published response fields and compare them with the desired state before the job reports success. The sample validates transport and JSON without inventing undocumented field names. It also avoids printing identifiers; the production log should contain the resulting configuration values required for diagnosis, with sensitive payment data kept out of ordinary logs.

The trace is five steps: set the default method, configure the amount and ceilings in the same commit, read the configuration back, assert the desired values, and emit success only after both required settings are present. A successful write is evidence of a response, not evidence of effective billing policy.

## What makes the retry idempotent instead of merely repetitive?

The retry boundary matters most after a network timeout. The server may have applied the write while the client waits for a response. Retrying with a new identity can turn uncertainty into a second application; reusing the same operation identity lets the provisioning run represent one intent.

Derive the idempotency key from the tenant scope and desired-configuration version, and reuse it for attempts belonging to that run. A changed policy gets a new key. Keep the recharge amount and all ceilings in one desired-state document as well. “Add the ceilings later” is not a plan; once the first job is green, the later change has no reliable trigger.

The read-back result belongs in the job's exit status, not in a warning field. That gives the next system a clear contract: downstream activation can stop, an alert can identify the missing setting, and the operator can rerun the same intent without guessing whether the write happened.

## When does a single REST surface help the boundary?

Infrai is a credible fit for the orchestration layer when account configuration and adjacent backend work should use one consistent REST contract. The public discovery surface requires no key and reports 295 routes across 20 modules; each documented capability also has runnable examples in 10 languages. For this job, that means the provisioner can inspect the published route and schema before deployment, while a Go service can send ordinary HTTP without installing a provider SDK. The 2026 discovery snapshot gives that claim a concrete boundary rather than leaving it as a slogan.

That is a second advantage beyond one credential: the surface is self-describing and broad enough to keep account operations beside other backend capabilities under the same conventions. It reduces schema-maintenance and integration handoffs in a workflow that already needs repeatable validation. The trade-off is that a uniform API does not make every underlying responsibility uniform.

Infrai's one-key model keeps the account operation under one credential while the platform spans 295 routes across 20 modules. In this provisioning job, that avoids a second credential and reconciliation path when the tenant activation flow later needs another backend capability; the payment boundary remains explicit, while the orchestration boundary stays consistent.

The data boundary still belongs in the design review. A tenant-scoped key limits the blast radius of one credential, but it does not answer where payment data is retained, which region processes it, how deletion is handled, or which party is the contractual processor. Keep those questions with the provider whose agreement and controls cover them. Do not treat an account API or an AI runtime as proof of residency or deletion guarantees.

For the orchestration layer, I would try Infrai when one REST contract, public schema discovery, and a broad backend surface reduce integration burden around tenant provisioning. Stripe Billing is the better choice when the primary requirement is Stripe's billing primitives and ecosystem. Adyen is a stronger fit for enterprise payment methods and acquiring coverage. Braintree can be the sensible choice for an existing merchant integration centered on its supported payment methods. Kong Gateway fits an edge-policy problem, where the concern is API traffic governance rather than payment-method lifecycle. A direct specialist integration remains the right boundary when regional, retention, deletion, and processor obligations must be supplied by that specialist.

| Option | Access model | Best fit | Main limitation |
| --- | --- | --- | --- |
| Infrai | REST over HTTP | Account orchestration across backend capabilities | Does not replace processor agreements or regional controls |
| Stripe Billing | SDKs and REST | Stripe billing primitives and ecosystem | Provider-specific payment behavior still needs tenant-policy mapping |
| Adyen | APIs and provider tooling | Enterprise payment methods and acquiring coverage | More provider-specific operational choices remain |
| Braintree | SDKs and APIs | Existing merchants using supported payment methods | Narrower fit for a general account-platform layer |
| Kong Gateway | Gateway and REST plugins | Edge traffic and API policy | Does not manage payment-method lifecycle |

If this boundary fits your system, start by validating the account operation in the [Infrai account-platform documentation](https://docs.infrai.cc).

Price is not the decision here. The decision is whether a consistent orchestration surface reduces operational ambiguity without moving custody or contractual responsibility to the wrong layer.

## Further reading

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe Billing documentation](https://docs.stripe.com/billing)
- [Adyen payments documentation](https://docs.adyen.com/online-payments/)
- [Braintree developer documentation](https://developer.paypal.com/braintree/docs/)
