# n8n Automation Layer — FerroFlux

Two workflows wire the Spark pipeline and portal to the n8n engine running on
`steel-network` at `http://n8n:5678`.

| File | Purpose | Trigger |
|------|---------|---------|
| `workflow_1_realtime_alerting.json` | Routes anomaly alerts (price spikes, sub-70% efficiency, factory onboarding) to Slack / Telegram / email per tenant | Inbound **webhook** from Spark |
| `workflow_2_market_harvest_cron.json` | Harvests external steel indices daily and publishes them to Kafka | **Cron** (daily 06:00) |

## 1. Import

In the n8n UI (`http://localhost:5678`):

1. **Workflows → Import from File** → select each JSON.
2. Open each imported workflow and attach credentials (they import without secrets):
   - **Workflow 1:** Slack (OAuth2), Telegram (Bot API), SMTP (Email Send).
   - **Workflow 2:** HTTP Request (your steel-index provider's API key) and **Kafka**.
3. **Activate** each workflow with the toggle once credentials are set.

## 2. Webhook URL (Workflow 1)

The webhook path is `steel-alert`, so the in-network URL the Spark jobs call is:

```
http://n8n:5678/webhook/steel-alert
```

Set this as `N8N_WEBHOOK_URL` for the Spark containers **and** the portal (see
`docker-compose.override.yml`). The Spark `etl_common.notify_n8n()` helper POSTs:

```json
{ "event_type": "price_spike",
  "payload": { "company_id": "EZZ", "factory_id": "EZZ_ALEX",
               "date": "2024-06-01", "price_change_pct": 8.4,
               "steel_price_egypt_egp": 41200 } }
```

`event_type` is one of `price_spike`, `low_efficiency` / `line_efficiency_low`,
or `factory_onboarded`. The **Route by Anomaly Type** Switch node maps these to
channels; extend it with a per-tenant lookup (company_id → channel/chat_id) for
true multi-tenant routing.

> Note: while a workflow is inactive you must use the **test** webhook URL
> (`/webhook-test/steel-alert`); the production `/webhook/...` path only responds
> once the workflow is activated.

## 3. Kafka credential (Workflow 2)

Create a **Kafka** credential in n8n:

| Field | Value |
|-------|-------|
| Brokers | `kafka:29092` |
| Client ID | `n8n-market-harvest` |
| SSL | off (internal network) |

The **Publish → Kafka** node writes to topic `steel_market_prices`, which is the
topic `scripts/kafka_producers/config.py` defines and which Spark Structured
Streaming (`streaming_etl.py`) consumes into the dashboard in real time.

Replace the placeholder provider URL in **Fetch Steel Indices**
(`https://api.example-steel-index.com/...`) with a real source and map its
response fields inside the **Scrub & Structure** Code node — the node already
falls back across common field names and rejects empty harvests.
