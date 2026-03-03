#!/bin/bash
# ──────────────────────────────────────────────────────────────────
# deploy.sh — Deploy the full video migration infrastructure
#
# Usage:
#   ./infra/deploy.sh <project> [region]
#
# Examples:
#   ./infra/deploy.sh ifrs
#   ./infra/deploy.sh indeed us-west-2
# ──────────────────────────────────────────────────────────────────

set -euo pipefail

PROJECT="${1:-}"
REGION="${2:-us-east-1}"

if [ -z "$PROJECT" ]; then
    echo "Usage: $0 <project-name> [region]"
    echo ""
    echo "Examples:"
    echo "  $0 ifrs"
    echo "  $0 indeed us-west-2"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "═══════════════════════════════════════════════════════"
echo "  Video Migration — Deploy"
echo "  Project: ${PROJECT}"
echo "  Region:  ${REGION}"
echo "═══════════════════════════════════════════════════════"
echo ""

# Check prerequisites
command -v aws >/dev/null 2>&1 || { echo "ERROR: AWS CLI not installed"; exit 1; }
command -v cdk >/dev/null 2>&1 || { echo "ERROR: AWS CDK not installed. Run: npm install -g aws-cdk"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: Python3 not found"; exit 1; }

# Get account ID
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "Account: ${ACCOUNT}"
echo ""

# Install Python dependencies
echo "→ Installing CDK dependencies..."
pip3 install -r requirements.txt -q

# Bootstrap CDK (idempotent — safe to run multiple times)
echo "→ Bootstrapping CDK..."
cdk bootstrap "aws://${ACCOUNT}/${REGION}"

# Synth first to validate
echo "→ Synthesizing CloudFormation templates..."
cdk synth --all -c project="${PROJECT}" -q

# Deploy all stacks
echo "→ Deploying all stacks..."
cdk deploy --all \
    -c project="${PROJECT}" \
    --require-approval broadening \
    --outputs-file "outputs-${PROJECT}.json"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Deployment complete!"
echo "  Outputs saved to: infra/outputs-${PROJECT}.json"
echo ""
echo "  Next steps:"
echo "    1. Update Secrets Manager with real credentials:"
echo "       aws secretsmanager update-secret \\"
echo "         --secret-id video-migration/${PROJECT}/kaltura \\"
echo "         --secret-string '{\"partner_id\":\"...\",\"admin_secret\":\"...\",\"user_id\":\"...\"}'"
echo ""
echo "    2. Run pilot: python pilot/pilot_runner.py --project ${PROJECT} --count 50"
echo "═══════════════════════════════════════════════════════"
