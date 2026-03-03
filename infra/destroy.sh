#!/bin/bash
# ──────────────────────────────────────────────────────────────────
# destroy.sh — Archive logs and tear down all infrastructure
#
# Usage:
#   ./infra/destroy.sh <project> [region]
#
# This script:
#   1. Archives CloudWatch logs to S3
#   2. Exports DynamoDB state to S3
#   3. Generates final audit report
#   4. Destroys all CDK stacks
# ──────────────────────────────────────────────────────────────────

set -euo pipefail

PROJECT="${1:-}"
REGION="${2:-us-east-1}"

if [ -z "$PROJECT" ]; then
    echo "Usage: $0 <project-name> [region]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "═══════════════════════════════════════════════════════"
echo "  Video Migration — Teardown"
echo "  Project: ${PROJECT}"
echo "  Region:  ${REGION}"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "⚠️  This will DESTROY all infrastructure for project '${PROJECT}'."
echo "    Archives will be saved first."
echo ""
read -p "Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[yY]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Step 1: Archive
echo ""
echo "→ Step 1: Archiving logs and state..."
python3 archive.py --project "${PROJECT}" --region "${REGION}"

# Step 2: Destroy CDK stacks
echo ""
echo "→ Step 2: Destroying CDK stacks..."
cdk destroy --all -c project="${PROJECT}" --force

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Teardown complete for project '${PROJECT}'."
echo ""
echo "  Archives saved to S3 bucket:"
echo "    video-migration-staging-${PROJECT}-*/archives/"
echo ""
echo "  Note: KMS key deletion is scheduled (30-day window)."
echo "  To cancel: aws kms cancel-key-deletion --key-id <key-id>"
echo "═══════════════════════════════════════════════════════"
