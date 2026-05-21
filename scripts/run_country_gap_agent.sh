#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 \"Country Name\""
  exit 1
fi

COUNTRY="$1"

PROMPT=$(cat <<EOF
Read scripts/country_gap_review_agent_prompt.md and execute the task for COUNTRY=${COUNTRY}.

Important:
- Work only on ${COUNTRY} unless a shared code path must be updated.
- Review the current gap evidence for ${COUNTRY} country-year-source_file.
- Modify what is necessary if a real fix is possible.
- If not fixable, document why the gap remains.
EOF
)

codex "$PROMPT"
