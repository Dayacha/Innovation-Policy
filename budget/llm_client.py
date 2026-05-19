"""
LLM client for the budget pipeline.

Thin adapter around reforms/llm_client.py — reuses all the retry logic,
cost tracking, and provider abstraction. Adds budget-specific operation
constants and a helper for JSON response parsing.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Optional

# Re-use the proven client from the reforms pipeline
# We resolve the path explicitly so budget works as a standalone import
_REFORMS_DIR = Path(__file__).resolve().parent.parent / "reforms"
if str(_REFORMS_DIR) not in sys.path:
    sys.path.insert(0, str(_REFORMS_DIR.parent))

from reforms.llm_client import LLMClient as _BaseLLMClient, PRICING  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = ["BudgetLLMClient", "PRICING"]


_TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")


class BudgetLLMClient(_BaseLLMClient):
    """LLM client with budget-pipeline operation constants and JSON helpers."""

    # Operation constants (extend the base set)
    OP_SCAN = "budget_scan"
    OP_EXTRACT = "budget_extract"
    OP_CONSISTENCY = "budget_consistency"
    OP_OTHER = "budget_other"

    def call_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: Optional[int] = None,
        operation: Optional[str] = None,
    ) -> dict:
        """Call LLM and parse the response as JSON.

        Returns the parsed dict on success, or {"items": [], "_parse_error": msg}
        on JSON parse failure (never raises — caller decides what to do).
        """
        raw = self.call(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
            operation=operation or self.OP_OTHER,
            json_mode=True,
        )
        # Strip markdown code fences if present (some models add them despite instructions)
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Drop first line (```json) and last line (```)
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        parsed = self._parse_json_lenient(text)
        if parsed is not None:
            return parsed

        try:
            json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed ({e}). Raw response (first 500 chars): {text[:500]}")
            return {"items": [], "_parse_error": str(e), "_raw": text[:1000]}
        return {"items": [], "_parse_error": "Unknown JSON parse failure", "_raw": text[:1000]}

    @staticmethod
    def _parse_json_lenient(text: str) -> Optional[dict]:
        """Best-effort parser for near-valid JSON emitted by the model."""
        candidates: list[str] = [text]

        start = text.find("{")
        if start != -1:
            candidates.append(text[start:])
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidates.append(text[start : end + 1])

        # Some scan responses fail due to a single trailing comma or a missing
        # closing bracket/brace near the end. Clean those before giving up.
        for candidate in list(candidates):
            cleaned = _TRAILING_COMMA_RE.sub(r"\1", candidate).strip()
            if cleaned != candidate:
                candidates.append(cleaned)
            balanced = BudgetLLMClient._balance_json_candidate(cleaned)
            if balanced != cleaned:
                candidates.append(balanced)

        decoder = json.JSONDecoder()
        for candidate in candidates:
            if not candidate:
                continue
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

            try:
                parsed, end_idx = decoder.raw_decode(candidate)
                trailing = candidate[end_idx:].strip()
                if isinstance(parsed, dict) and not trailing:
                    return parsed
            except json.JSONDecodeError:
                continue

        return None

    @staticmethod
    def _balance_json_candidate(text: str) -> str:
        """Append missing JSON closers when the payload is truncated at the end."""
        stack: list[str] = []
        in_string = False
        escaped = False

        for ch in text:
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch in "{[":
                stack.append(ch)
            elif ch == "}" and stack and stack[-1] == "{":
                stack.pop()
            elif ch == "]" and stack and stack[-1] == "[":
                stack.pop()

        if in_string:
            text += '"'
        if not stack:
            return text

        closers = "".join("}" if opener == "{" else "]" for opener in reversed(stack))
        return text + closers

    @classmethod
    def from_config(cls, config: dict, usage_file: Optional[Path] = None) -> "BudgetLLMClient":
        """Construct from a config dict that has an 'llm' block."""
        from budget.config import LLM_USAGE_FILE, DEFAULT_LLM_CONFIG

        # Merge defaults under config['llm'] if missing
        merged = {**DEFAULT_LLM_CONFIG, **config.get("llm", {})}
        effective_config = {**config, "llm": merged}

        if usage_file is None:
            usage_file = LLM_USAGE_FILE

        return cls(config=effective_config, usage_file=usage_file)

    def switch_model(self, model: str) -> None:
        """Swap the active model (used to switch between scan and extract models)."""
        self.model = model
        # Re-initialise the underlying client with new model (provider unchanged)
        self._init_client()
        logger.debug(f"Switched LLM model to: {model}")
