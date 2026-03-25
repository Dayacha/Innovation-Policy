"""
LLM API client supporting Anthropic (Claude) and OpenAI.

Provides a unified interface for making LLM calls with automatic
retries, rate limiting, and detailed cost tracking by operation type.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Pricing per 1M tokens (as of early 2025 — update as needed)
PRICING = {
    # Anthropic models
    "claude-sonnet-4-20250514": {"input": 3.00, "output": 15.00},
    "claude-sonnet-4-5-20250929": {"input": 3.00, "output": 15.00},
    "claude-opus-4-20250514": {"input": 15.00, "output": 75.00},
    "claude-3-5-sonnet-20241022": {"input": 3.00, "output": 15.00},
    "claude-3-haiku-20240307": {"input": 0.25, "output": 1.25},
    # OpenAI models
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
}


class LLMClient:
    """Unified client for LLM API calls with detailed usage tracking."""

    # Operation type constants
    OP_EXTRACTION = "extraction"
    OP_WITHIN_SURVEY_DEDUP = "within_survey_dedup"
    OP_CROSS_SURVEY_DEDUP = "cross_survey_dedup"
    OP_OTHER = "other"

    def __init__(self, config, usage_file=None):
        """Initialize the LLM client.

        Args:
            config: Configuration dict with llm settings.
            usage_file: Optional path to save usage data. If None, uses
                        data/output/llm_usage.json (or suffixed version).
        """
        llm_config = config.get("llm", {})
        self.provider = llm_config.get("provider", "anthropic")
        self.model = llm_config.get("model", "claude-sonnet-4-20250514")
        self.max_tokens = llm_config.get("max_tokens", 4096)
        self.temperature = llm_config.get("temperature", 0)
        self.api_delay = config.get("processing", {}).get("api_delay", 1.0)

        # Resolve API key from config or environment
        self.api_key = llm_config.get("api_key", "")
        if not self.api_key:
            if self.provider == "anthropic":
                self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            elif self.provider == "openai":
                self.api_key = os.environ.get("OPENAI_API_KEY", "")

        if not self.api_key:
            raise ValueError(
                f"No API key found for provider '{self.provider}'. "
                f"Set it in config.yaml or via environment variable "
                f"({'ANTHROPIC_API_KEY' if self.provider == 'anthropic' else 'OPENAI_API_KEY'})."
            )

        # Initialize the appropriate client
        self._client = None
        self._init_client()

        # Determine usage file path
        if usage_file:
            self.usage_file = Path(usage_file)
        else:
            # load_config() in run_pipeline.py already applies output_suffix
            # to config["paths"]["output"], so use that path directly.
            output_dir = config.get("paths", {}).get("output", "data/output")
            self.usage_file = Path(output_dir) / "llm_usage.json"

        # Aggregate usage tracking (legacy, for backward compatibility)
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_calls = 0

        # Detailed usage tracking by operation and survey
        self.usage_records = []
        self._current_survey = None  # Set by caller via set_current_survey()

        # Load existing usage data if file exists
        self._load_existing_usage()

    def _init_client(self):
        """Initialize the provider-specific client."""
        if self.provider == "anthropic":
            import anthropic
            self._client = anthropic.Anthropic(api_key=self.api_key)
        elif self.provider == "openai":
            import openai
            self._client = openai.OpenAI(api_key=self.api_key)
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")

    def _load_existing_usage(self):
        """Load existing usage data from file if it exists."""
        if self.usage_file.exists():
            try:
                with open(self.usage_file, "r") as f:
                    data = json.load(f)
                    self.usage_records = data.get("records", [])
                    # Restore aggregate totals
                    for rec in self.usage_records:
                        self.total_input_tokens += rec.get("input_tokens", 0)
                        self.total_output_tokens += rec.get("output_tokens", 0)
                        self.total_calls += 1
                    logger.info(
                        f"Loaded {len(self.usage_records)} existing usage records"
                    )
            except Exception as e:
                logger.warning(f"Could not load existing usage data: {e}")

    def set_current_survey(self, country_code, survey_year):
        """Set the current survey context for usage tracking.

        Args:
            country_code: ISO 3166-1 alpha-3 country code.
            survey_year: Year of the survey being processed.
        """
        self._current_survey = {
            "country_code": country_code,
            "survey_year": survey_year,
        }

    def call(self, system_prompt, user_prompt, max_tokens=None, operation=None):
        """Make an LLM API call.

        Args:
            system_prompt: System/instruction prompt.
            user_prompt: User message with the actual content to process.
            max_tokens: Override default max_tokens for this call.
            operation: Operation type for usage tracking. Use class constants:
                       OP_EXTRACTION, OP_WITHIN_SURVEY_DEDUP, OP_CROSS_SURVEY_DEDUP.

        Returns:
            The LLM's text response.
        """
        max_tokens = max_tokens or self.max_tokens
        operation = operation or self.OP_OTHER

        try:
            start_time = time.time()

            if self.provider == "anthropic":
                response, input_tokens, output_tokens = self._call_anthropic(
                    system_prompt, user_prompt, max_tokens
                )
            elif self.provider == "openai":
                response, input_tokens, output_tokens = self._call_openai(
                    system_prompt, user_prompt, max_tokens
                )
            else:
                raise ValueError(f"Unknown provider: {self.provider}")

            elapsed = time.time() - start_time

            # Update aggregate totals
            self.total_input_tokens += input_tokens
            self.total_output_tokens += output_tokens
            self.total_calls += 1

            # Record detailed usage
            self._record_usage(
                operation=operation,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                elapsed_seconds=elapsed,
            )

            time.sleep(self.api_delay)
            return response

        except Exception as e:
            logger.error(f"LLM API call failed: {e}")
            raise

    def _record_usage(self, operation, input_tokens, output_tokens, elapsed_seconds):
        """Record a usage entry with full context."""
        record = {
            "timestamp": datetime.now().isoformat(),
            "provider": self.provider,
            "model": self.model,
            "operation": operation,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "cost_usd": self._calculate_cost(input_tokens, output_tokens),
        }

        # Add survey context if available
        if self._current_survey:
            record["country_code"] = self._current_survey["country_code"]
            record["survey_year"] = self._current_survey["survey_year"]

        self.usage_records.append(record)

    def _calculate_cost(self, input_tokens, output_tokens):
        """Calculate cost in USD for a given number of tokens."""
        pricing = PRICING.get(self.model, {"input": 0, "output": 0})
        input_cost = (input_tokens / 1_000_000) * pricing["input"]
        output_cost = (output_tokens / 1_000_000) * pricing["output"]
        return round(input_cost + output_cost, 6)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=60),
    )
    def _call_anthropic(self, system_prompt, user_prompt, max_tokens):
        """Call the Anthropic API with prompt caching for the system prompt.

        The system prompt is marked as cacheable — Anthropic caches it for 5
        minutes across calls in the same process, reducing input token cost by
        ~90% for surveys that require many chunks (the typical case).
        """
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=self.temperature,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {"role": "user", "content": user_prompt},
            ],
        )

        input_tokens = 0
        output_tokens = 0
        if hasattr(response, "usage"):
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            # Cache read tokens are billed at 10% of normal input price — still
            # count them as input_tokens so cost tracking stays accurate.
            cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
            cache_write = getattr(response.usage, "cache_creation_input_tokens", 0) or 0
            # input_tokens already includes cache_read in the Anthropic SDK,
            # so we only add cache_write overhead (billed at 125% for first write).
            input_tokens += cache_write  # account for cache-write surcharge tokens

        return response.content[0].text, input_tokens, output_tokens

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=60),
    )
    def _call_openai(self, system_prompt, user_prompt, max_tokens):
        """Call the OpenAI API."""
        response = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=self.temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        input_tokens = 0
        output_tokens = 0
        if response.usage:
            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens

        return response.choices[0].message.content, input_tokens, output_tokens

    def save_usage(self):
        """Save usage data to JSON file."""
        self.usage_file.parent.mkdir(parents=True, exist_ok=True)

        # Build summary by operation
        summary_by_op = {}
        for rec in self.usage_records:
            op = rec.get("operation", "other")
            if op not in summary_by_op:
                summary_by_op[op] = {
                    "calls": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "cost_usd": 0,
                    "elapsed_seconds": 0,
                }
            summary_by_op[op]["calls"] += 1
            summary_by_op[op]["input_tokens"] += rec.get("input_tokens", 0)
            summary_by_op[op]["output_tokens"] += rec.get("output_tokens", 0)
            summary_by_op[op]["total_tokens"] += rec.get("total_tokens", 0)
            summary_by_op[op]["cost_usd"] += rec.get("cost_usd", 0)
            summary_by_op[op]["elapsed_seconds"] += rec.get("elapsed_seconds", 0)

        # Round costs
        for op in summary_by_op:
            summary_by_op[op]["cost_usd"] = round(summary_by_op[op]["cost_usd"], 4)
            summary_by_op[op]["elapsed_seconds"] = round(
                summary_by_op[op]["elapsed_seconds"], 2
            )

        # Build summary by survey
        summary_by_survey = {}
        for rec in self.usage_records:
            key = f"{rec.get('country_code', 'unknown')}_{rec.get('survey_year', 'unknown')}"
            if key not in summary_by_survey:
                summary_by_survey[key] = {
                    "country_code": rec.get("country_code"),
                    "survey_year": rec.get("survey_year"),
                    "calls": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "cost_usd": 0,
                }
            summary_by_survey[key]["calls"] += 1
            summary_by_survey[key]["input_tokens"] += rec.get("input_tokens", 0)
            summary_by_survey[key]["output_tokens"] += rec.get("output_tokens", 0)
            summary_by_survey[key]["total_tokens"] += rec.get("total_tokens", 0)
            summary_by_survey[key]["cost_usd"] += rec.get("cost_usd", 0)

        # Round costs in survey summary
        for key in summary_by_survey:
            summary_by_survey[key]["cost_usd"] = round(
                summary_by_survey[key]["cost_usd"], 4
            )

        data = {
            "generated_at": datetime.now().isoformat(),
            "provider": self.provider,
            "model": self.model,
            "pricing_per_million": PRICING.get(self.model, {}),
            "summary": {
                "total_calls": self.total_calls,
                "total_input_tokens": self.total_input_tokens,
                "total_output_tokens": self.total_output_tokens,
                "total_tokens": self.total_input_tokens + self.total_output_tokens,
                "total_cost_usd": round(
                    sum(r.get("cost_usd", 0) for r in self.usage_records), 4
                ),
            },
            "by_operation": summary_by_op,
            "by_survey": list(summary_by_survey.values()),
            "records": self.usage_records,
        }

        with open(self.usage_file, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Usage data saved to {self.usage_file}")

    def get_usage_summary(self):
        """Return a summary of API usage."""
        return {
            "provider": self.provider,
            "model": self.model,
            "total_calls": self.total_calls,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens,
            "total_cost_usd": round(
                sum(r.get("cost_usd", 0) for r in self.usage_records), 4
            ),
        }

    def print_usage_report(self):
        """Print a formatted usage report to the console."""
        summary = self.get_usage_summary()

        print("\n" + "=" * 60)
        print("LLM USAGE REPORT")
        print("=" * 60)
        print(f"Provider: {summary['provider']}")
        print(f"Model: {summary['model']}")
        print(f"Total API calls: {summary['total_calls']:,}")
        print(f"Total tokens: {summary['total_tokens']:,}")
        print(f"  - Input: {summary['total_input_tokens']:,}")
        print(f"  - Output: {summary['total_output_tokens']:,}")
        print(f"Estimated cost: ${summary['total_cost_usd']:.4f}")

        # By operation breakdown
        print("\n--- By Operation ---")
        ops = {}
        for rec in self.usage_records:
            op = rec.get("operation", "other")
            if op not in ops:
                ops[op] = {"calls": 0, "tokens": 0, "cost": 0}
            ops[op]["calls"] += 1
            ops[op]["tokens"] += rec.get("total_tokens", 0)
            ops[op]["cost"] += rec.get("cost_usd", 0)

        for op, data in sorted(ops.items(), key=lambda x: -x[1]["cost"]):
            pct = (data["cost"] / summary["total_cost_usd"] * 100) if summary["total_cost_usd"] > 0 else 0
            print(
                f"  {op}: {data['calls']} calls, "
                f"{data['tokens']:,} tokens, "
                f"${data['cost']:.4f} ({pct:.1f}%)"
            )

        print("=" * 60 + "\n")
