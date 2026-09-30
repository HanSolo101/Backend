"""Mock Salesforce Bulk API v2 Server.

Simulates Salesforce OAuth2 authentication and Bulk API v2 endpoints:
- POST /services/oauth2/token          → OAuth2 token
- POST /services/data/vXX.0/jobs/query → Create bulk query job
- GET  /services/data/vXX.0/jobs/query/{job_id} → Get job status
- GET  /services/data/vXX.0/jobs/query/{job_id}/results → Get results
- PATCH /services/data/vXX.0/jobs/query/{job_id} → Abort job

Generates realistic synthetic data for 10+ Salesforce objects using Faker.
"""

import uuid
import time
import random
import asyncio
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from faker import Faker

fake = Faker()

app = FastAPI(title="Mock Salesforce Bulk API v2", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── In-memory state ───
_jobs: dict[str, dict] = {}
_tokens: dict[str, dict] = {}

# Rate limit simulation: 1 in 8 requests will return 429
RATE_LIMIT_PROBABILITY = 0.12


# ─── Salesforce Object Schemas & Data Generators ───

SALESFORCE_OBJECTS = {
    "Account": {
        "fields": ["Id", "Name", "Industry", "AnnualRevenue", "NumberOfEmployees", "BillingCity", "BillingState", "BillingCountry", "Phone", "Website", "Type", "CreatedDate", "LastModifiedDate"],
        "generator": lambda i: {
            "Id": f"001{fake.bothify('??########')}",
            "Name": fake.company(),
            "Industry": random.choice(["Technology", "Healthcare", "Finance", "Manufacturing", "Retail", "Energy", "Education"]),
            "AnnualRevenue": round(random.uniform(100000, 50000000), 2),
            "NumberOfEmployees": random.randint(10, 50000),
            "BillingCity": fake.city(),
            "BillingState": fake.state_abbr(),
            "BillingCountry": "US",
            "Phone": fake.phone_number(),
            "Website": fake.url(),
            "Type": random.choice(["Customer", "Prospect", "Partner", "Competitor"]),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(30, 1800))).isoformat() + "Z",
            "LastModifiedDate": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat() + "Z",
        },
    },
    "Contact": {
        "fields": ["Id", "FirstName", "LastName", "Email", "Phone", "Title", "Department", "AccountId", "MailingCity", "MailingState", "LeadSource", "CreatedDate", "LastModifiedDate"],
        "generator": lambda i: {
            "Id": f"003{fake.bothify('??########')}",
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),
            "Email": fake.email(),
            "Phone": fake.phone_number(),
            "Title": fake.job(),
            "Department": random.choice(["Sales", "Engineering", "Marketing", "Finance", "HR", "Operations"]),
            "AccountId": f"001{fake.bothify('??########')}",
            "MailingCity": fake.city(),
            "MailingState": fake.state_abbr(),
            "LeadSource": random.choice(["Web", "Referral", "Partner", "Conference", "Cold Call"]),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(30, 1200))).isoformat() + "Z",
            "LastModifiedDate": (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat() + "Z",
        },
    },
    "Opportunity": {
        "fields": ["Id", "Name", "StageName", "Amount", "CloseDate", "Probability", "AccountId", "Type", "LeadSource", "ForecastCategory", "IsClosed", "IsWon", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"006{fake.bothify('??########')}",
            "Name": f"{fake.company()} - {fake.bs().title()}",
            "StageName": random.choice(["Prospecting", "Qualification", "Needs Analysis", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]),
            "Amount": round(random.uniform(5000, 2000000), 2),
            "CloseDate": (datetime.utcnow() + timedelta(days=random.randint(-60, 180))).strftime("%Y-%m-%d"),
            "Probability": random.choice([10, 20, 30, 50, 70, 80, 90, 100]),
            "AccountId": f"001{fake.bothify('??########')}",
            "Type": random.choice(["New Business", "Existing Business", "Renewal"]),
            "LeadSource": random.choice(["Web", "Referral", "Partner", "Outbound"]),
            "ForecastCategory": random.choice(["Pipeline", "Best Case", "Commit", "Closed"]),
            "IsClosed": random.choice([True, False]),
            "IsWon": random.choice([True, False]),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(10, 365))).isoformat() + "Z",
        },
    },
    "Lead": {
        "fields": ["Id", "FirstName", "LastName", "Email", "Company", "Title", "Status", "LeadSource", "Industry", "Rating", "Phone", "City", "State", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"00Q{fake.bothify('??########')}",
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),
            "Email": fake.email(),
            "Company": fake.company(),
            "Title": fake.job(),
            "Status": random.choice(["Open", "Contacted", "Qualified", "Unqualified", "Converted"]),
            "LeadSource": random.choice(["Web", "Referral", "Partner", "Advertisement", "Cold Call"]),
            "Industry": random.choice(["Technology", "Finance", "Healthcare", "Manufacturing"]),
            "Rating": random.choice(["Hot", "Warm", "Cold"]),
            "Phone": fake.phone_number(),
            "City": fake.city(),
            "State": fake.state_abbr(),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(5, 500))).isoformat() + "Z",
        },
    },
    "Task": {
        "fields": ["Id", "Subject", "Status", "Priority", "WhoId", "WhatId", "ActivityDate", "Description", "OwnerId", "IsHighPriority", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"00T{fake.bothify('??########')}",
            "Subject": random.choice(["Follow up call", "Send proposal", "Schedule meeting", "Review contract", "Send quote", "Demo preparation"]),
            "Status": random.choice(["Not Started", "In Progress", "Completed", "Deferred", "Waiting"]),
            "Priority": random.choice(["High", "Normal", "Low"]),
            "WhoId": f"003{fake.bothify('??########')}",
            "WhatId": f"006{fake.bothify('??########')}",
            "ActivityDate": (datetime.utcnow() + timedelta(days=random.randint(-30, 60))).strftime("%Y-%m-%d"),
            "Description": fake.sentence(),
            "OwnerId": f"005{fake.bothify('??########')}",
            "IsHighPriority": random.choice([True, False]),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(1, 200))).isoformat() + "Z",
        },
    },
    "Case": {
        "fields": ["Id", "Subject", "Status", "Priority", "Origin", "Type", "Reason", "AccountId", "ContactId", "Description", "IsClosed", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"500{fake.bothify('??########')}",
            "Subject": fake.sentence(nb_words=5),
            "Status": random.choice(["New", "Working", "Escalated", "Closed"]),
            "Priority": random.choice(["High", "Medium", "Low"]),
            "Origin": random.choice(["Phone", "Email", "Web", "Chat"]),
            "Type": random.choice(["Problem", "Feature Request", "Question"]),
            "Reason": random.choice(["Installation", "Performance", "Billing", "Other"]),
            "AccountId": f"001{fake.bothify('??########')}",
            "ContactId": f"003{fake.bothify('??########')}",
            "Description": fake.paragraph(),
            "IsClosed": random.choice([True, False]),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(1, 400))).isoformat() + "Z",
        },
    },
    "Product2": {
        "fields": ["Id", "Name", "ProductCode", "Description", "Family", "IsActive", "QuantityUnitOfMeasure", "StockKeepingUnit", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"01t{fake.bothify('??########')}",
            "Name": fake.catch_phrase(),
            "ProductCode": fake.bothify("PRD-####"),
            "Description": fake.sentence(),
            "Family": random.choice(["Hardware", "Software", "Services", "Subscription"]),
            "IsActive": random.choice([True, False]),
            "QuantityUnitOfMeasure": random.choice(["Each", "License", "Hour", "Month"]),
            "StockKeepingUnit": fake.bothify("SKU-########"),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(100, 1000))).isoformat() + "Z",
        },
    },
    "PricebookEntry": {
        "fields": ["Id", "Name", "Pricebook2Id", "Product2Id", "UnitPrice", "IsActive", "UseStandardPrice", "CurrencyIsoCode", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"01u{fake.bothify('??########')}",
            "Name": fake.catch_phrase(),
            "Pricebook2Id": f"01s{fake.bothify('??########')}",
            "Product2Id": f"01t{fake.bothify('??########')}",
            "UnitPrice": round(random.uniform(9.99, 9999.99), 2),
            "IsActive": True,
            "UseStandardPrice": random.choice([True, False]),
            "CurrencyIsoCode": "USD",
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(50, 800))).isoformat() + "Z",
        },
    },
    "Contract": {
        "fields": ["Id", "AccountId", "Status", "StartDate", "EndDate", "ContractTerm", "OwnerExpirationNotice", "ContractNumber", "Description", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"800{fake.bothify('??########')}",
            "AccountId": f"001{fake.bothify('??########')}",
            "Status": random.choice(["Draft", "InApproval", "Activated", "Terminated", "Expired"]),
            "StartDate": (datetime.utcnow() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
            "EndDate": (datetime.utcnow() + timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d"),
            "ContractTerm": random.choice([6, 12, 24, 36]),
            "OwnerExpirationNotice": random.choice(["30", "60", "90", "120"]),
            "ContractNumber": fake.bothify("CN-########"),
            "Description": fake.sentence(),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(30, 365))).isoformat() + "Z",
        },
    },
    "Asset": {
        "fields": ["Id", "Name", "AccountId", "ContactId", "Product2Id", "SerialNumber", "Status", "InstallDate", "PurchaseDate", "Quantity", "Price", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"02i{fake.bothify('??########')}",
            "Name": fake.catch_phrase(),
            "AccountId": f"001{fake.bothify('??########')}",
            "ContactId": f"003{fake.bothify('??########')}",
            "Product2Id": f"01t{fake.bothify('??########')}",
            "SerialNumber": fake.bothify("SN-############"),
            "Status": random.choice(["Purchased", "Shipped", "Installed", "Registered", "Obsolete"]),
            "InstallDate": (datetime.utcnow() - timedelta(days=random.randint(10, 500))).strftime("%Y-%m-%d"),
            "PurchaseDate": (datetime.utcnow() - timedelta(days=random.randint(10, 600))).strftime("%Y-%m-%d"),
            "Quantity": random.randint(1, 100),
            "Price": round(random.uniform(100, 50000), 2),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(10, 500))).isoformat() + "Z",
        },
    },
    "Campaign": {
        "fields": ["Id", "Name", "Type", "Status", "StartDate", "EndDate", "BudgetedCost", "ActualCost", "ExpectedRevenue", "NumberSent", "NumberOfLeads", "NumberOfContacts", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"701{fake.bothify('??########')}",
            "Name": f"{fake.bs().title()} Campaign {random.randint(2024,2026)}",
            "Type": random.choice(["Email", "Webinar", "Conference", "Advertisement", "Social Media"]),
            "Status": random.choice(["Planned", "In Progress", "Completed", "Aborted"]),
            "StartDate": (datetime.utcnow() - timedelta(days=random.randint(10, 200))).strftime("%Y-%m-%d"),
            "EndDate": (datetime.utcnow() + timedelta(days=random.randint(10, 100))).strftime("%Y-%m-%d"),
            "BudgetedCost": round(random.uniform(5000, 200000), 2),
            "ActualCost": round(random.uniform(3000, 180000), 2),
            "ExpectedRevenue": round(random.uniform(10000, 500000), 2),
            "NumberSent": random.randint(100, 50000),
            "NumberOfLeads": random.randint(10, 2000),
            "NumberOfContacts": random.randint(5, 1000),
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(10, 200))).isoformat() + "Z",
        },
    },
    "Event": {
        "fields": ["Id", "Subject", "StartDateTime", "EndDateTime", "Location", "WhoId", "WhatId", "Description", "IsAllDayEvent", "OwnerId", "CreatedDate"],
        "generator": lambda i: {
            "Id": f"00U{fake.bothify('??########')}",
            "Subject": random.choice(["Client Meeting", "Product Demo", "Quarterly Review", "Strategy Session", "Training Workshop"]),
            "StartDateTime": (datetime.utcnow() + timedelta(days=random.randint(-30, 60), hours=random.randint(8, 17))).isoformat() + "Z",
            "EndDateTime": (datetime.utcnow() + timedelta(days=random.randint(-30, 60), hours=random.randint(9, 18))).isoformat() + "Z",
            "Location": fake.address(),
            "WhoId": f"003{fake.bothify('??########')}",
            "WhatId": f"006{fake.bothify('??########')}",
            "Description": fake.sentence(),
            "IsAllDayEvent": random.choice([True, False]),
            "OwnerId": f"005{fake.bothify('??########')}",
            "CreatedDate": (datetime.utcnow() - timedelta(days=random.randint(1, 100))).isoformat() + "Z",
        },
    },
}


def generate_records(object_name: str, count: int) -> list[dict]:
    """Generate synthetic records for a Salesforce object."""
    obj = SALESFORCE_OBJECTS.get(object_name)
    if not obj:
        return []
    return [obj["generator"](i) for i in range(count)]


# ─── OAuth2 Endpoint ───

class TokenRequest(BaseModel):
    grant_type: str = "client_credentials"
    client_id: str = "mock_client_id"
    client_secret: str = "mock_client_secret"


@app.post("/services/oauth2/token")
async def oauth2_token(request: TokenRequest = None):
    """Issue a mock OAuth2 access token."""
    token = f"mock_sf_token_{uuid.uuid4().hex[:16]}"
    _tokens[token] = {
        "issued_at": datetime.utcnow().isoformat(),
        "expires_in": 7200,
    }
    return {
        "access_token": token,
        "instance_url": "http://localhost:8100",
        "token_type": "Bearer",
        "issued_at": str(int(time.time() * 1000)),
        "signature": "mock_signature",
    }


# ─── Bulk API v2 Endpoints ───

class BulkJobRequest(BaseModel):
    operation: str = "query"
    query: Optional[str] = None
    object: Optional[str] = None
    contentType: str = "CSV"
    columnDelimiter: str = "COMMA"
    lineEnding: str = "LF"


@app.post("/services/data/v59.0/jobs/query")
async def create_bulk_job(request: BulkJobRequest):
    """Create a new Bulk API v2 query job."""
    # Simulate rate limiting
    if random.random() < RATE_LIMIT_PROBABILITY:
        raise HTTPException(
            status_code=429,
            detail="REQUEST_LIMIT_EXCEEDED",
            headers={"Retry-After": "2"},
        )

    # Parse object name from SOQL query
    object_name = request.object
    if not object_name and request.query:
        # Extract from "SELECT ... FROM ObjectName"
        parts = request.query.upper().split("FROM")
        if len(parts) > 1:
            object_name = parts[1].strip().split()[0]
            # Find matching case-sensitive name
            for key in SALESFORCE_OBJECTS:
                if key.upper() == object_name:
                    object_name = key
                    break

    if object_name not in SALESFORCE_OBJECTS:
        raise HTTPException(status_code=400, detail=f"Unknown object: {object_name}")

    record_count = random.randint(200, 2000)
    job_id = str(uuid.uuid4())

    _jobs[job_id] = {
        "id": job_id,
        "operation": request.operation,
        "object": object_name,
        "state": "UploadComplete",
        "createdDate": datetime.utcnow().isoformat() + "Z",
        "systemModstamp": datetime.utcnow().isoformat() + "Z",
        "concurrencyMode": "Parallel",
        "contentType": request.contentType,
        "apiVersion": 59.0,
        "jobType": "V2Query",
        "lineEnding": request.lineEnding,
        "columnDelimiter": request.columnDelimiter,
        "numberRecordsProcessed": 0,
        "retries": 0,
        "totalProcessingTime": 0,
        "_target_count": record_count,
        "_created_time": time.time(),
        "_processing_seconds": random.uniform(1.0, 4.0),
    }

    return {
        "id": job_id,
        "operation": request.operation,
        "object": object_name,
        "state": "UploadComplete",
        "createdDate": _jobs[job_id]["createdDate"],
        "contentType": request.contentType,
        "apiVersion": 59.0,
        "jobType": "V2Query",
    }


@app.get("/services/data/v59.0/jobs/query/{job_id}")
async def get_job_status(job_id: str):
    """Get the status of a Bulk API v2 job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]

    # Simulate processing time
    elapsed = time.time() - job["_created_time"]
    if elapsed >= job["_processing_seconds"] and job["state"] != "Aborted":
        job["state"] = "JobComplete"
        job["numberRecordsProcessed"] = job["_target_count"]
        job["totalProcessingTime"] = int(elapsed * 1000)

    return {
        "id": job["id"],
        "operation": job["operation"],
        "object": job["object"],
        "state": job["state"],
        "createdDate": job["createdDate"],
        "systemModstamp": datetime.utcnow().isoformat() + "Z",
        "concurrencyMode": job["concurrencyMode"],
        "contentType": job["contentType"],
        "apiVersion": job["apiVersion"],
        "jobType": job["jobType"],
        "numberRecordsProcessed": job["numberRecordsProcessed"],
        "retries": job["retries"],
        "totalProcessingTime": job["totalProcessingTime"],
    }


@app.get("/services/data/v59.0/jobs/query/{job_id}/results")
async def get_job_results(
    job_id: str,
    maxRecords: int = Query(default=2000),
    locator: Optional[str] = Query(default=None),
):
    """Retrieve results of a completed Bulk API v2 job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]

    if job["state"] != "JobComplete":
        raise HTTPException(status_code=400, detail=f"Job state is {job['state']}, not JobComplete")

    object_name = job["object"]
    total = job["_target_count"]

    # Pagination via locator
    offset = 0
    if locator:
        try:
            offset = int(locator)
        except ValueError:
            offset = 0

    remaining = total - offset
    batch_size = min(maxRecords, remaining)

    records = generate_records(object_name, batch_size)

    next_locator = None
    if offset + batch_size < total:
        next_locator = str(offset + batch_size)

    return {
        "records": records,
        "sforce-locator": next_locator,
        "sforce-numberofrecords": batch_size,
        "done": next_locator is None,
    }


@app.patch("/services/data/v59.0/jobs/query/{job_id}")
async def abort_job(job_id: str):
    """Abort a running Bulk API v2 job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    _jobs[job_id]["state"] = "Aborted"
    return {"id": job_id, "state": "Aborted"}


@app.get("/services/data/v59.0/jobs/query")
async def list_jobs():
    """List all Bulk API v2 jobs."""
    return {
        "done": True,
        "records": [
            {
                "id": j["id"],
                "operation": j["operation"],
                "object": j["object"],
                "state": j["state"],
                "createdDate": j["createdDate"],
                "numberRecordsProcessed": j["numberRecordsProcessed"],
            }
            for j in _jobs.values()
        ],
        "nextRecordsUrl": None,
    }


@app.get("/api/objects")
async def list_available_objects():
    """List all available Salesforce objects and their field schemas."""
    return {
        name: {"fields": obj["fields"], "sample": obj["generator"](0)}
        for name, obj in SALESFORCE_OBJECTS.items()
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "mock-salesforce", "objects": list(SALESFORCE_OBJECTS.keys())}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)
