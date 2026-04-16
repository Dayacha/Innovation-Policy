"""
budget — LLM-based Finance Bill R&D budget extraction pipeline.

Completely separate from the rule-based budget/ pipeline.
Sends PDF text directly to an LLM for structured extraction.

Entry point:
    from budget.pipeline import run_pipeline
    run_pipeline(config)
"""
