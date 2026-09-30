"""Mock HubSpot API Server.

Simulates HubSpot CRM API endpoints for:
- Contacts, Companies, Deals, Tickets, Line Items, Engagements, Pipelines, Owners

Provides paginated endpoints with cursor-based pagination and incremental
updates via `updated_after` filter to support dlt incremental loading.
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from faker import Faker

fake = Faker()

app = FastAPI(title="Mock HubSpot API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── Data Generators ───

def _ts(days_ago: int) -> str:
    """Generate ISO timestamp N days ago."""
    return (datetime.utcnow() - timedelta(days=days_ago)).isoformat() + "Z"


def _epoch_ms(days_ago: int) -> int:
    """Generate epoch milliseconds N days ago."""
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return int(dt.timestamp() * 1000)


HUBSPOT_GENERATORS = {
    "contacts": lambda: {
        "id": str(random.randint(100000, 999999)),
        "properties": {
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "company": fake.company(),
            "jobtitle": fake.job(),
            "lifecyclestage": random.choice(["subscriber", "lead", "marketingqualifiedlead", "salesqualifiedlead", "opportunity", "customer"]),
            "hs_lead_status": random.choice(["NEW", "OPEN", "IN_PROGRESS", "CONNECTED", "UNQUALIFIED"]),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "createdate": _ts(random.randint(10, 800)),
            "lastmodifieddate": _ts(random.randint(0, 10)),
            "hs_object_id": str(random.randint(100000, 999999)),
        },
        "createdAt": _ts(random.randint(10, 800)),
        "updatedAt": _ts(random.randint(0, 10)),
    },
    "companies": lambda: {
        "id": str(random.randint(200000, 299999)),
        "properties": {
            "name": fake.company(),
            "domain": fake.domain_name(),
            "industry": random.choice(["TECHNOLOGY", "FINANCE", "HEALTHCARE", "MANUFACTURING", "RETAIL"]),
            "annualrevenue": str(round(random.uniform(100000, 50000000), 2)),
            "numberofemployees": str(random.randint(10, 10000)),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US",
            "phone": fake.phone_number(),
            "website": fake.url(),
            "type": random.choice(["PROSPECT", "PARTNER", "RESELLER", "VENDOR", "OTHER"]),
            "createdate": _ts(random.randint(30, 1000)),
            "lastmodifieddate": _ts(random.randint(0, 15)),
            "hs_object_id": str(random.randint(200000, 299999)),
        },
        "createdAt": _ts(random.randint(30, 1000)),
        "updatedAt": _ts(random.randint(0, 15)),
    },
    "deals": lambda: {
        "id": str(random.randint(300000, 399999)),
        "properties": {
            "dealname": f"{fake.company()} - {fake.bs().title()}",
            "dealstage": random.choice(["appointmentscheduled", "qualifiedtobuy", "presentationscheduled", "decisionmakerboughtin", "contractsent", "closedwon", "closedlost"]),
            "amount": str(round(random.uniform(1000, 500000), 2)),
            "closedate": (datetime.utcnow() + timedelta(days=random.randint(-30, 180))).isoformat() + "Z",
            "pipeline": "default",
            "dealtype": random.choice(["newbusiness", "existingbusiness"]),
            "hs_priority": random.choice(["low", "medium", "high"]),
            "createdate": _ts(random.randint(5, 365)),
            "lastmodifieddate": _ts(random.randint(0, 5)),
            "hs_object_id": str(random.randint(300000, 399999)),
        },
        "createdAt": _ts(random.randint(5, 365)),
        "updatedAt": _ts(random.randint(0, 5)),
    },
    "tickets": lambda: {
        "id": str(random.randint(400000, 499999)),
        "properties": {
            "subject": fake.sentence(nb_words=5),
            "content": fake.paragraph(),
            "hs_pipeline": "0",
            "hs_pipeline_stage": random.choice(["1", "2", "3", "4"]),
            "hs_ticket_priority": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "hs_ticket_category": random.choice(["PRODUCT_ISSUE", "BILLING_ISSUE", "FEATURE_REQUEST", "GENERAL_INQUIRY"]),
            "createdate": _ts(random.randint(1, 200)),
            "lastmodifieddate": _ts(random.randint(0, 5)),
            "hs_object_id": str(random.randint(400000, 499999)),
        },
        "createdAt": _ts(random.randint(1, 200)),
        "updatedAt": _ts(random.randint(0, 5)),
    },
    "line_items": lambda: {
        "id": str(random.randint(500000, 599999)),
        "properties": {
            "name": fake.catch_phrase(),
            "quantity": str(random.randint(1, 50)),
            "price": str(round(random.uniform(10, 5000), 2)),
            "amount": str(round(random.uniform(10, 250000), 2)),
            "discount": str(round(random.uniform(0, 30), 1)),
            "hs_product_id": str(random.randint(600000, 699999)),
            "createdate": _ts(random.randint(5, 300)),
            "lastmodifieddate": _ts(random.randint(0, 10)),
            "hs_object_id": str(random.randint(500000, 599999)),
        },
        "createdAt": _ts(random.randint(5, 300)),
        "updatedAt": _ts(random.randint(0, 10)),
    },
    "engagements": lambda: {
        "id": str(random.randint(700000, 799999)),
        "properties": {
            "hs_engagement_type": random.choice(["NOTE", "EMAIL", "TASK", "MEETING", "CALL"]),
            "hs_engagement_source": random.choice(["CRM_UI", "API", "INTEGRATION"]),
            "hs_timestamp": _ts(random.randint(0, 200)),
            "hs_body_preview": fake.sentence(),
            "hs_engagement_source_id": str(random.randint(100, 9999)),
            "createdate": _ts(random.randint(1, 200)),
            "lastmodifieddate": _ts(random.randint(0, 5)),
            "hs_object_id": str(random.randint(700000, 799999)),
        },
        "createdAt": _ts(random.randint(1, 200)),
        "updatedAt": _ts(random.randint(0, 5)),
    },
    "pipelines": lambda: {
        "id": str(random.randint(800, 899)),
        "label": random.choice(["Sales Pipeline", "Enterprise Pipeline", "SMB Pipeline", "Partner Pipeline"]),
        "displayOrder": random.randint(0, 5),
        "stages": [
            {"id": str(i), "label": stage, "displayOrder": i}
            for i, stage in enumerate(["Appointment Scheduled", "Qualified to Buy", "Presentation Scheduled", "Decision Maker Bought-In", "Contract Sent", "Closed Won", "Closed Lost"])
        ],
        "createdAt": _ts(random.randint(100, 500)),
        "updatedAt": _ts(random.randint(0, 30)),
    },
    "owners": lambda: {
        "id": str(random.randint(900000, 999999)),
        "email": fake.email(),
        "firstName": fake.first_name(),
        "lastName": fake.last_name(),
        "userId": random.randint(1000, 9999),
        "teams": [{"id": str(random.randint(1, 10)), "name": random.choice(["Sales Team", "Support Team", "Marketing Team"])}],
        "createdAt": _ts(random.randint(200, 1000)),
        "updatedAt": _ts(random.randint(0, 30)),
    },
}

# Pre-generate pool of records for consistency
_data_pools: dict[str, list] = {}
POOL_SIZE = 500

def _ensure_pool(resource: str):
    """Generate a data pool for a resource if not already done."""
    if resource not in _data_pools:
        gen = HUBSPOT_GENERATORS.get(resource)
        if gen:
            _data_pools[resource] = [gen() for _ in range(POOL_SIZE)]


@app.get("/crm/v3/objects/{resource}")
async def list_objects(
    resource: str,
    limit: int = Query(default=100, le=100),
    after: Optional[str] = Query(default=None),
    properties: Optional[str] = Query(default=None),
):
    """List CRM objects with cursor-based pagination."""
    _ensure_pool(resource)
    pool = _data_pools.get(resource, [])

    offset = 0
    if after:
        try:
            offset = int(after)
        except ValueError:
            offset = 0

    batch = pool[offset : offset + limit]
    next_after = None
    if offset + limit < len(pool):
        next_after = str(offset + limit)

    return {
        "results": batch,
        "paging": {
            "next": {"after": next_after, "link": f"/crm/v3/objects/{resource}?after={next_after}"} if next_after else None,
        },
    }


@app.get("/crm/v3/pipelines/{object_type}")
async def list_pipelines(object_type: str):
    """List pipelines for an object type (deals, tickets)."""
    _ensure_pool("pipelines")
    return {"results": _data_pools.get("pipelines", [])[:5]}


@app.get("/crm/v3/owners")
async def list_owners(
    limit: int = Query(default=100),
    after: Optional[str] = Query(default=None),
):
    """List HubSpot owners."""
    _ensure_pool("owners")
    pool = _data_pools["owners"]

    offset = int(after) if after else 0
    batch = pool[offset : offset + limit]
    next_after = str(offset + limit) if offset + limit < len(pool) else None

    return {
        "results": batch,
        "paging": {
            "next": {"after": next_after} if next_after else None,
        },
    }


@app.get("/api/resources")
async def list_available_resources():
    """List available HubSpot resources."""
    return {
        "resources": list(HUBSPOT_GENERATORS.keys()),
        "pool_size": POOL_SIZE,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "mock-hubspot", "resources": list(HUBSPOT_GENERATORS.keys())}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8200)
