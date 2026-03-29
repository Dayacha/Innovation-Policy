"""Lightweight AI client abstraction for post-processing validation batches."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List
import re
from json import JSONDecodeError

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency until installed
    OpenAI = None  # type: ignore


class MissingAPIKeyError(RuntimeError):
    """Raised when the API key is missing."""


class MissingOpenAIDependencyError(RuntimeError):
    """Raised when openai package is not installed."""


@dataclass
class AIClientConfig:
    model: str = "gpt-4o-mini"
    temperature: float = 0.1
    max_output_tokens: int = 1500
    api_key_env: str = "OPENAI_API_KEY"


class AIClient:
    """Wrapper around an OpenAI-style chat/completions API."""

    def __init__(self, config: AIClientConfig | None = None):
        self.config = config or AIClientConfig()

        if OpenAI is None:
            raise MissingOpenAIDependencyError(
                "openai package is not installed. Add it to requirements and install dependencies."
            )

        api_key = os.getenv(self.config.api_key_env)
        if not api_key:
            raise MissingAPIKeyError(
                f"API key missing. Set {self.config.api_key_env} in your environment to enable AI validation."
            )

        self.client = OpenAI(api_key=api_key)

    def build_messages(self, batch: List[dict]) -> list[dict]:
        """Construct chat messages for the batch of candidate records."""
        instructions = (
            "You are validating pre-extracted government budget items from finance bills. "
            "Goal: determine whether each line is a real budget item relevant to research/innovation "
            "and capture the plausible invested amount. "
            "Use only the provided fields and structured budget context. Do not invent data or infer beyond them. "
            "For each record, clean the original-language description, translate that cleaned description to English, "
            "validate that it is a real budget item, adjust the amount if the description contradicts it, classify R&D relevance, "
            "and return strict JSON only. "
            "Ignore totals, ministry-wide aggregates, revenue lines, or legal-reference-only lines. "
            "Preserve record_id for alignment. When budget_window is provided, use section/program/item metadata, "
            "previous_lines, next_lines, and neighbor_amounts to decide whether the current line is a real item or a subtotal/header. "
            "Treat raw_page_text_excerpt as secondary fallback context only. "
            "Do NOT wrap the JSON in markdown code fences or any extra text."
        )

        fields_instruction = (
            "Required JSON keys per record: record_id, keep (bool), clean_program_code, "
            "clean_program_description_da, clean_program_description_en, clean_budget_type_da, clean_budget_type_en, "
            "validated_amount_local, currency, ai_rd_category, ai_pillar, ai_confidence, ai_decision, ai_rationale, parse_issue. "
            "Categories: ai_rd_category in [direct_rd, innovation_system, possible_rd, not_rd]; "
            "ai_pillar in [Direct R&D, Innovation, Ambiguous, Exclude]; ai_decision in [include, review, exclude]. "
            "Confidence 0-1. parse_issue options: none, legal_reference_noise, merged_adjacent_items, malformed_budget_type, "
            "missing_program_code, amount_alignment_uncertain, duplicate_candidate, other."
        )

        batch_payload = []
        for record in batch:
            # Keep raw dict but ensure JSON-serializable.
            batch_payload.append(record)

        user_content = {
            "task": "validate_budget_items",
            "items": batch_payload,
            "requirements": {
                "return_format": "JSON array, same order as items",
                "language_notes": "Input may be multilingual; provide English translation for the cleaned description only",
            },
        }

        return [
            {"role": "system", "content": instructions},
            {"role": "system", "content": fields_instruction},
            {
                "role": "user",
                "content": (
                    "Process these items and return only JSON (no prose). "
                    "Respond with a JSON array where each element maps to the input order.\n" + json.dumps(user_content)
                ),
            },
        ]

    def validate_batch(self, batch: List[dict]) -> list[dict]:
        """Send a batch to the model and return parsed JSON list."""
        messages = self.build_messages(batch)
        response = self.client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
        )

        content = response.choices[0].message.content if response.choices else ""
        if not content:
            raise ValueError("Empty response from AI model")

        def extract_json(txt: str) -> str:
            txt = txt.strip()
            # Case 1: fenced block somewhere in the message
            if "```" in txt:
                parts = txt.split("```")
                # pick the part inside the first fence if possible
                if len(parts) >= 3:
                    candidate = parts[1]
                    # drop a leading language token like json
                    candidate = re.sub(r"^\\s*json\\s*", "", candidate, flags=re.IGNORECASE)
                    return candidate.strip()
            # Case 2: find first JSON array/object
            for opener in ["[", "{"]:
                idx = txt.find(opener)
                if idx != -1:
                    return txt[idx:].strip()
            return txt

        cleaned = extract_json(content)
        # Try strict parse, then a lenient fallback (strip trailing commas, fix quotes)
        try:
            parsed = json.loads(cleaned)
        except JSONDecodeError:
            repaired = re.sub(r",\\s*}", "}", cleaned)
            repaired = re.sub(r",\\s*]", "]", repaired)
            # remove stray backticks
            repaired = repaired.replace("`", "")
            try:
                parsed = json.loads(repaired)
            except JSONDecodeError as exc:  # pragma: no cover - runtime path
                raise ValueError(f"Failed to parse AI JSON: {exc}: {content[:200]}")

        if not isinstance(parsed, list):
            raise ValueError("AI response is not a list; ensure model returns JSON array.")

        return parsed
