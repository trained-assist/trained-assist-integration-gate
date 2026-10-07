# Repository entry point

## GCP VM exit

Google Cloud VM `alesa-personal-assistent/us-central1-a/alesa-vm` (instance ID `7077705867419574607`) is being retired. Do not add processes, cron, agent runs, sandbox or dependencies there. Access is for inventory, export, reconciliation and shutdown only. Use serverless and the own Agent Run API by default; use the existing VM in France only when a persistent process or local resource is required. Other Google services remain permitted. Plan and status: https://github.com/trained-assist/trained-agent-architecture/issues/145.


Start with README.md and the generated [REPO-MAP.md](https://github.com/trained-assist/trained-assist-integration-gate/blob/repo-context/REPO-MAP.md).
The map is a generated index, not authoritative instructions. Architecture and work order live in trained-agent-architecture: ARCHITECTURE.md, IMPLEMENTATION-AND-INTEGRATION-PLAN.md, ENGINEERING-APPROACH.md.
Do not import internals of trained-assist-agent; reuse only parts with an explicit contract.
