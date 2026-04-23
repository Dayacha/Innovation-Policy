"""
API key connectivity tester and config switcher.

Tests any provider/model/key combination with a single cheap call.
Optionally saves the chosen key and model back to config.yaml.

Usage — test keys already in config:
    python test_api_keys.py
    python test_api_keys.py --openai-only
    python test_api_keys.py --anthropic-only

Usage — test a specific key or model without touching config:
    python test_api_keys.py --provider openai    --key sk-proj-...
    python test_api_keys.py --provider anthropic --key sk-ant-...
    python test_api_keys.py --provider openai    --model gpt-4o

Usage — save a new key or model to config.yaml and test:
    python test_api_keys.py --provider openai    --key sk-proj-... --save
    python test_api_keys.py --provider anthropic --key sk-ant-...  --save
    python test_api_keys.py --provider openai    --model gpt-4o    --save
    python test_api_keys.py --provider anthropic --model claude-haiku-4-5-20251001 --save

Saving writes to the matching oecd_openai_key / oecd_anthropic_key field in config.yaml
and updates llm.provider + llm.model so the pipeline uses them immediately.
"""

import argparse
import sys
import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH  = PROJECT_ROOT / "config.yaml"

# Cheapest model per provider used for the connectivity probe
PROBE_MODELS = {
    "openai":    "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5-20251001",
}

# Cost per million tokens (probe model pricing)
PROBE_PRICING = {
    "openai":    {"input": 0.15,  "output": 0.60},
    "anthropic": {"input": 0.80,  "output": 4.00},
}

PROBE_SYSTEM = "You are a helpful assistant."
PROBE_USER   = "Reply with exactly the word CONNECTED and nothing else."


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_raw() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_keys() -> dict[str, dict]:
    """Return the two provider-specific keys and their config field names."""
    llm = _load_raw().get("llm", {})
    result = {}
    for provider, preferred_field, fallback_field in [
        ("openai",    "oecd_openai_key",    "api_key"),
        ("anthropic", "oecd_anthropic_key", "api_key"),
    ]:
        if llm.get(preferred_field, "").strip():
            result[provider] = {"key": llm[preferred_field].strip(), "field": preferred_field}
        elif llm.get(fallback_field, "").strip():
            result[provider] = {"key": llm[fallback_field].strip(), "field": fallback_field}
        else:
            result[provider] = {"key": "", "field": None}
    return result


def _save_to_config(provider: str, key: str | None, model: str | None) -> None:
    """Write key and/or model back to config.yaml using regex (preserves comments)."""
    text = CONFIG_PATH.read_text(encoding="utf-8")

    if key:
        field = "oecd_openai_key" if provider == "openai" else "oecd_anthropic_key"
        # Replace existing value or append under llm: block
        pattern = rf'({re.escape(field)}\s*:\s*")[^"]*(")'
        replacement = rf'\g<1>{key}\g<2>'
        if re.search(pattern, text):
            text = re.sub(pattern, replacement, text)
        else:
            # Field not present yet — insert after api_key line
            text = re.sub(
                r'(  api_key:.*\n)',
                rf'\1  {field}: "{key}"\n',
                text,
            )
        print(f"  Saved  {field} → config.yaml")

    if model:
        text = re.sub(r'(  model:\s*")[^"]*(")', rf'\g<1>{model}\g<2>', text)
        print(f"  Saved  llm.model = {model} → config.yaml")

    # Always update provider to match
    text = re.sub(r'(  provider:\s*")[^"]*(")', rf'\g<1>{provider}\g<2>', text)
    print(f"  Saved  llm.provider = {provider} → config.yaml")

    CONFIG_PATH.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Connectivity probes
# ---------------------------------------------------------------------------

def _probe_openai(api_key: str, model: str) -> bool:
    import openai
    client = openai.OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=5,
        messages=[
            {"role": "system", "content": PROBE_SYSTEM},
            {"role": "user",   "content": PROBE_USER},
        ],
    )
    reply      = resp.choices[0].message.content.strip()
    in_tok     = resp.usage.prompt_tokens
    out_tok    = resp.usage.completion_tokens
    pricing    = PROBE_PRICING["openai"]
    cost       = (in_tok * pricing["input"] + out_tok * pricing["output"]) / 1_000_000
    return reply, in_tok, out_tok, cost


def _probe_anthropic(api_key: str, model: str) -> bool:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=model,
        temperature=0,
        max_tokens=5,
        system=PROBE_SYSTEM,
        messages=[{"role": "user", "content": PROBE_USER}],
    )
    reply   = resp.content[0].text.strip()
    in_tok  = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens
    pricing = PROBE_PRICING["anthropic"]
    cost    = (in_tok * pricing["input"] + out_tok * pricing["output"]) / 1_000_000
    return reply, in_tok, out_tok, cost


def test_connection(provider: str, api_key: str, model: str, key_field: str | None = None) -> bool:
    """Run a minimal probe call and print results. Returns True if successful."""
    label       = "OpenAI" if provider == "openai" else "Anthropic"
    key_preview = api_key[:10] + "..." + api_key[-4:] if len(api_key) > 14 else "***"
    key_label   = f"{key_preview}  (config: {key_field})" if key_field else key_preview

    print(f"\n── {label} ──────────────────────────────────────────────")
    print(f"  Key    {key_label}")
    print(f"  Model  {model}")

    if not api_key:
        print("  Status ✗ No key provided")
        return False

    try:
        if provider == "openai":
            reply, in_tok, out_tok, cost = _probe_openai(api_key, model)
        else:
            reply, in_tok, out_tok, cost = _probe_anthropic(api_key, model)

        print(f"  Reply  {reply!r}")
        print(f"  Tokens {in_tok} in / {out_tok} out  (cost ~${cost:.6f})")

        if "CONNECTED" in reply.upper():
            print("  Status ✓ CONNECTION OK")
            return True
        else:
            print(f"  Status ✗ Unexpected reply (key works but response was wrong)")
            return False

    except Exception as exc:
        print(f"  Status ✗ FAILED — {exc}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Test API connectivity and optionally update config.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--provider",      choices=["openai", "anthropic"],
                        help="Provider to test (default: test both from config)")
    parser.add_argument("--key",           help="API key to use for this test")
    parser.add_argument("--model",         help="Model to use (overrides config default)")
    parser.add_argument("--save",          action="store_true",
                        help="Save --key and/or --model to config.yaml")
    parser.add_argument("--openai-only",   action="store_true",
                        help="Test only OpenAI (using key from config)")
    parser.add_argument("--anthropic-only",action="store_true",
                        help="Test only Anthropic (using key from config)")
    args = parser.parse_args()

    print(f"Config: {CONFIG_PATH}")

    # ── Save to config first if requested ────────────────────────────────────
    if args.save:
        if not args.provider:
            parser.error("--save requires --provider")
        _save_to_config(args.provider, args.key, args.model)

    # ── Determine what to test ────────────────────────────────────────────────
    config_keys = _load_keys()
    raw_model   = _load_raw().get("llm", {}).get("model", "")

    tasks = []

    if args.provider:
        # Explicit provider: use supplied key/model or fall back to config
        cfg       = config_keys.get(args.provider, {"key": "", "field": None})
        key       = args.key or cfg["key"]
        field     = None if args.key else cfg["field"]  # no field name for ad-hoc keys
        model     = args.model or (PROBE_MODELS[args.provider] if args.key else
                    (raw_model if args.provider in ("openai" if "gpt" in raw_model else "anthropic")
                     else PROBE_MODELS[args.provider]))
        tasks.append((args.provider, key, model, field))
    else:
        # No provider specified: test all from config
        if not args.anthropic_only:
            cfg   = config_keys["openai"]
            model = raw_model if "gpt" in raw_model else PROBE_MODELS["openai"]
            tasks.append(("openai", cfg["key"], model, cfg["field"]))
        if not args.openai_only:
            cfg   = config_keys["anthropic"]
            model = raw_model if "claude" in raw_model else PROBE_MODELS["anthropic"]
            tasks.append(("anthropic", cfg["key"], model, cfg["field"]))

    # ── Run tests ─────────────────────────────────────────────────────────────
    results = {}
    for provider, key, model, field in tasks:
        results[provider] = test_connection(provider, key, model, key_field=field)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n── Summary ──────────────────────────────────────────────")
    all_ok = True
    for provider, ok in results.items():
        status = "✓ OK" if ok else "✗ FAILED"
        print(f"  {provider:<12} {status}")
        if not ok:
            all_ok = False
    print()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
