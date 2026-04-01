# Cross-Verification Cost Report
## OECD Innovation Reform Extraction Pipeline

---

## 1. Purpose

This report evaluates the cost-quality tradeoff of three approaches to cross-verification for the OECD innovation reform extraction pipeline. The pipeline extracts policy reform mentions from OECD country surveys spanning 38 countries. Two corpus-size assumptions are used throughout: a **targeted run** of 10 surveys per country (380 surveys total), and a **full corpus** of approximately 600 surveys (1995–2025). Cross-verification — running extraction more than once, either with the same or different models — can improve recall and provide a documented quality assurance layer. The three strategies differ in cost, correction coverage, and implementation effort. Costs are benchmarked against the DNK 2021 survey (~190 pages, 25 extraction chunks), which is representative of the corpus in length and complexity.

---

## 2. Baseline Costs (Single Run + Pass 2 Adjudicator)

The Pass 2 adjudicator already runs on all models using gpt-4o-mini; it re-checks borderline rows only and is negligible in cost.

| Model | Cost/survey | 10 surveys/country (380) | Full corpus (600) |
|---|---|---|---|
| gpt-4o-mini | $0.0333 | ~$13 | ~$20 |
| gpt-4o | $0.5461 | ~$208 | ~$328 |
| claude-sonnet-4 | $0.6632 | ~$252 | ~$398 |
| claude-opus-4 | $3.7026 | ~$1,407 | ~$2,222 |

Reforms extracted on DNK 2021 (one survey): gpt-4o-mini = 12, gpt-4o = 5, claude-sonnet-4 = 13, claude-opus-4 = 9. This spread illustrates meaningful model-level disagreement and motivates cross-verification.

---

## 3. Strategy A: Same Model, Double Run

Each survey is processed twice with the same model. The two result sets are compared; reforms that appear in one run but not the other are flagged, and a lightweight integration call (~$0.01/survey with gpt-4o-mini) produces a merged consensus list.

At temperature=0, the same model is highly deterministic, so genuine disagreements between runs are rare. The primary value of this strategy is documenting reproducibility rather than recovering missed reforms. It is most appropriate when an audit trail is needed for publication purposes.

| Model | Cost/survey | 10 surveys/country (380) | Full corpus (600) |
|---|---|---|---|
| gpt-4o-mini | ~$0.077 | ~$29 | ~$46 |
| gpt-4o | ~$1.10 | ~$418 | ~$660 |
| claude-sonnet-4 | ~$1.34 | ~$509 | ~$804 |
| claude-opus-4 | ~$7.42 | ~$2,820 | ~$4,452 |

Cost multiplier: approximately 2.05x the base extraction cost (double extraction plus trivial integration).

---

## 4. Strategy B: Two Different Models with Integration

Each survey is processed once with model A and once with model B. The union of extracted reforms is compared; reforms found by one model but not the other (estimated at 30–50% of the total, based on the benchmark spread) are passed to a third, lightweight model for integration into a merged consensus list (~$0.05/survey).

This is the approach most closely aligned with the cross-validation principle Sébastien's team described, and it is already partially implemented via the `adjudicator_model` configuration option. It provides genuine recall improvement because different models have different extraction patterns, as confirmed by the benchmark.

All six unique pairings are shown below, sorted by full-corpus cost. Integration in all cases uses gpt-4o-mini (~$0.05/survey).

| # | Model A | Model B | Cost/survey | 10 surveys/country (380) | Full corpus (600) | Notes |
|---|---|---|---|---|---|---|
| 1 | gpt-4o-mini | gpt-4o | ~$0.63 | ~$239 | ~$378 | Best cost/quality tradeoff; cross-provider |
| 2 | gpt-4o-mini | claude-sonnet-4 | ~$0.75 | ~$285 | ~$448 | Cross-provider; sonnet has highest raw recall |
| 3 | gpt-4o | claude-sonnet-4 | ~$1.26 | ~$479 | ~$756 | Both frontier mid-tier; strongest consensus signal |
| 4 | gpt-4o-mini | claude-opus-4 | ~$3.79 | ~$1,440 | ~$2,271 | Cheapest extractor + highest-quality validator |
| 5 | gpt-4o | claude-opus-4 | ~$4.30 | ~$1,634 | ~$2,579 | High quality, high cost |
| 6 | claude-sonnet-4 | claude-opus-4 | ~$4.42 | ~$1,680 | ~$2,650 | Two Anthropic models; limited cross-provider diversity |

Cost/survey = cost(Model A) + cost(Model B) + $0.05 integration, from benchmark data.

The benchmark showed recall varying from 5 (gpt-4o) to 13 (claude-sonnet-4) reforms on the same survey, which means cross-provider pairings (OpenAI + Anthropic) are likely to surface more genuine disagreements than same-provider pairings. Pairings 1–3 represent the practical range: below $1.30/survey they cover the main quality-cost spectrum without committing to opus-4 pricing.

---

## 5. Strategy C: Single Run with Sample Validation

The full corpus is processed once with a primary model. A random stratified sample of 10–20% of surveys (120 surveys at 20%) is then re-run with a more capable model to estimate precision and recall of the primary run. No merge step is applied to the full dataset — the sample produces a validation report only, not a corrected dataset.

This strategy is appropriate when the goal is quality estimation rather than correction of every reform in the corpus.

All combinations where the validator is more capable than the primary model are shown below, sorted by total cost. Sample size is 20% of each corpus; sample cost = sample surveys × validator cost/survey.

| Primary model | Validator model | Primary cost | Sample cost | Total — 380 surveys | Total — 600 surveys |
|---|---|---|---|---|---|
| gpt-4o-mini | gpt-4o | ~$13 / ~$20 | 76×$0.55 = ~$42 / 120×$0.55 = ~$66 | ~$54 | ~$86 |
| gpt-4o-mini | claude-sonnet-4 | ~$13 / ~$20 | 76×$0.66 = ~$50 / 120×$0.66 = ~$80 | ~$63 | ~$100 |
| gpt-4o-mini | claude-opus-4 | ~$13 / ~$20 | 76×$3.70 = ~$281 / 120×$3.70 = ~$444 | ~$294 | ~$464 |
| gpt-4o | claude-sonnet-4 | ~$208 / ~$328 | 76×$0.66 = ~$50 / 120×$0.66 = ~$80 | ~$258 | ~$408 |
| gpt-4o | claude-opus-4 | ~$208 / ~$328 | 76×$3.70 = ~$281 / 120×$3.70 = ~$444 | ~$489 | ~$772 |
| claude-sonnet-4 | claude-opus-4 | ~$252 / ~$398 | 76×$3.70 = ~$281 / 120×$3.70 = ~$444 | ~$533 | ~$842 |

The sample produces labeled ground-truth data points — the validator's decisions on the sample can be treated as a gold standard against which the primary model's recall and precision are measured. The labeled sample is also reusable: it can serve as a test set for future prompt iterations or model changes without incurring additional cost.

The cheapest meaningful option is gpt-4o-mini primary + gpt-4o validator at ~$86. Note however that gpt-4o extracted only 5 reforms vs. gpt-4o-mini's 12 on the benchmark, so gpt-4o may undercount rather than over-count — making it a conservative rather than strict upper-bound validator. gpt-4o-mini + claude-sonnet-4 at ~$100 is only marginally more expensive and uses a model with the highest observed recall (13 reforms), making it a stronger upper bound for precision/recall estimation.

---

## 6. Comparison Summary

| Strategy | Model(s) | 380 surveys | 600 surveys | QA level | Correction coverage | Effort |
|---|---|---|---|---|---|---|
| Baseline | gpt-4o-mini | ~$13 | ~$20 | Low | Borderline rows only | Done |
| Baseline | gpt-4o | ~$208 | ~$328 | Low | Borderline rows only | Done |
| Baseline | claude-sonnet-4 | ~$252 | ~$398 | Low | Borderline rows only | Done |
| Baseline | claude-opus-4 | ~$1,407 | ~$2,222 | Low | Borderline rows only | Done |
| A: Double run | gpt-4o-mini × 2 | ~$29 | ~$46 | Low | Reproducibility only | Config change |
| A: Double run | gpt-4o × 2 | ~$418 | ~$660 | Low | Reproducibility only | Config change |
| A: Double run | claude-sonnet-4 × 2 | ~$509 | ~$804 | Low | Reproducibility only | Config change |
| A: Double run | claude-opus-4 × 2 | ~$2,820 | ~$4,452 | Low | Reproducibility only | Config change |
| B: Two models | mini + gpt-4o | ~$239 | ~$378 | Medium-High | All reforms, union+integration | Partial |
| B: Two models | mini + sonnet-4 | ~$285 | ~$448 | Medium-High | All reforms, union+integration | Partial |
| B: Two models | gpt-4o + sonnet-4 | ~$479 | ~$756 | High | All reforms, union+integration | Partial |
| B: Two models | mini + opus-4 | ~$1,440 | ~$2,271 | High | All reforms, union+integration | Partial |
| B: Two models | gpt-4o + opus-4 | ~$1,634 | ~$2,579 | High | All reforms, union+integration | Partial |
| B: Two models | sonnet-4 + opus-4 | ~$1,680 | ~$2,650 | High | All reforms, union+integration | Partial |
| C: Sample (20%) | mini → gpt-4o | ~$54 | ~$86 | Medium | 20% sample, reusable labels | New code |
| C: Sample (20%) | mini → sonnet-4 | ~$63 | ~$100 | Medium-High | 20% sample, reusable labels | New code |
| C: Sample (20%) | gpt-4o → sonnet-4 | ~$258 | ~$408 | Medium-High | 20% sample, reusable labels | New code |
| C: Sample (20%) | mini → opus-4 | ~$294 | ~$464 | High | 20% sample, reusable labels | New code |
| C: Sample (20%) | gpt-4o → opus-4 | ~$489 | ~$772 | High | 20% sample, reusable labels | New code |
| C: Sample (20%) | sonnet-4 → opus-4 | ~$533 | ~$842 | High | 20% sample, reusable labels | New code |

---

## 7. Recommendation

For the current production run, Strategy B with the budget pairing (gpt-4o-mini + gpt-4o + gpt-4o-mini integration, ~$378) gives the best cost-quality tradeoff and aligns with the cross-validation approach already partially in place via the `adjudicator_model` config. If the goal is a quality audit rather than correcting the full dataset, Strategy C with gpt-4o-mini primary + claude-sonnet-4 validator is the strongest option at ~$100: it is only $14 more than the cheapest C option, uses the model with the highest observed recall as the upper-bound validator, and produces a reusable labeled sample that can serve as a test set for future prompt changes without additional cost. Strategy A adds minimal quality value at temperature=0 and is only warranted when a documented reproducibility audit trail is required for publication. In all cases, the Pass 2 adjudicator already provides a within-model cross-check on borderline rows at negligible cost and should remain enabled regardless of which strategy is chosen.
