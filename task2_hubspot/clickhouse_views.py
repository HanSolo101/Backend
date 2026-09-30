"""HubSpot ClickHouse Views & Table Loader.

Creates ClickHouse tables from HubSpot Parquet data landed in MinIO
and builds curated analytical views for business reporting.
"""

import structlog
from shared.clickhouse_client import get_clickhouse_client

logger = structlog.get_logger(__name__)

# HubSpot resource → ClickHouse table schema
HUBSPOT_SCHEMAS = {
    "contacts": {
        "id": "String", "firstname": "String", "lastname": "String",
        "email": "String", "phone": "String", "company": "String",
        "jobtitle": "String", "lifecyclestage": "String",
        "hs_lead_status": "String", "city": "String", "state": "String",
        "country": "String", "createdate": "String", "lastmodifieddate": "String",
        "hs_object_id": "String", "_created_at": "String", "_updated_at": "String",
    },
    "companies": {
        "id": "String", "name": "String", "domain": "String",
        "industry": "String", "annualrevenue": "String",
        "numberofemployees": "String", "city": "String", "state": "String",
        "country": "String", "phone": "String", "website": "String",
        "type": "String", "createdate": "String", "lastmodifieddate": "String",
        "hs_object_id": "String", "_created_at": "String", "_updated_at": "String",
    },
    "deals": {
        "id": "String", "dealname": "String", "dealstage": "String",
        "amount": "String", "closedate": "String", "pipeline": "String",
        "dealtype": "String", "hs_priority": "String",
        "createdate": "String", "lastmodifieddate": "String",
        "hs_object_id": "String", "_created_at": "String", "_updated_at": "String",
    },
    "tickets": {
        "id": "String", "subject": "String", "content": "String",
        "hs_pipeline": "String", "hs_pipeline_stage": "String",
        "hs_ticket_priority": "String", "hs_ticket_category": "String",
        "createdate": "String", "lastmodifieddate": "String",
        "hs_object_id": "String", "_created_at": "String", "_updated_at": "String",
    },
    "line_items": {
        "id": "String", "name": "String", "quantity": "String",
        "price": "String", "amount": "String", "discount": "String",
        "hs_product_id": "String", "createdate": "String",
        "lastmodifieddate": "String", "hs_object_id": "String",
        "_created_at": "String", "_updated_at": "String",
    },
    "engagements": {
        "id": "String", "hs_engagement_type": "String",
        "hs_engagement_source": "String", "hs_timestamp": "String",
        "hs_body_preview": "String", "hs_engagement_source_id": "String",
        "createdate": "String", "lastmodifieddate": "String",
        "hs_object_id": "String", "_created_at": "String", "_updated_at": "String",
    },
    "pipelines": {
        "id": "String", "label": "String", "displayOrder": "Int32",
        "stages": "String", "_created_at": "String", "_updated_at": "String",
    },
    "owners": {
        "id": "String", "email": "String", "firstName": "String",
        "lastName": "String", "userId": "Int32", "teams": "String",
        "_created_at": "String", "_updated_at": "String",
    },
}


class HubSpotClickHouseViews:
    """Manages ClickHouse tables and analytical views for HubSpot data."""

    def __init__(self):
        self.ch = get_clickhouse_client()

    def create_all_tables(self):
        """Create ClickHouse tables for all HubSpot resources."""
        for resource, schema in HUBSPOT_SCHEMAS.items():
            table_name = f"bronze_hubspot_{resource}"
            try:
                self.ch.create_table(
                    table_name=table_name,
                    columns=schema,
                    order_by=["id"],
                )
                logger.info(f"[CH] Table created: {table_name}")
            except Exception as e:
                logger.warning(f"[CH] Table creation issue: {table_name}", error=str(e))

    def insert_records(self, resource: str, records: list[dict]):
        """Insert records into the corresponding ClickHouse table."""
        table_name = f"bronze_hubspot_{resource}"
        schema = HUBSPOT_SCHEMAS.get(resource, {})
        columns = list(schema.keys())

        # Coerce records to match schema
        coerced = []
        for record in records:
            row = {}
            for col in columns:
                val = record.get(col)
                expected_type = schema.get(col, "String")
                if val is None:
                    row[col] = val
                elif expected_type == "Int32":
                    try:
                        row[col] = int(val)
                    except (ValueError, TypeError):
                        row[col] = 0
                elif expected_type == "Float64":
                    try:
                        row[col] = float(val)
                    except (ValueError, TypeError):
                        row[col] = 0.0
                else:
                    if isinstance(val, (list, dict)):
                        import json
                        row[col] = json.dumps(val, default=str)
                    else:
                        row[col] = str(val) if val is not None else None
            coerced.append(row)

        self.ch.insert_data(table_name, coerced, columns)
        logger.info(f"[CH] Inserted {len(coerced)} rows into {table_name}")

    def create_analytical_views(self):
        """Create curated ClickHouse analytical views for HubSpot data."""
        views = {
            "v_hubspot_deal_pipeline": """
                SELECT
                    d.id as deal_id,
                    d.dealname,
                    d.dealstage,
                    d.amount,
                    d.closedate,
                    d.pipeline,
                    d.dealtype,
                    d.hs_priority,
                    d.createdate
                FROM bronze_hubspot_deals d
                ORDER BY d.createdate DESC
            """,
            "v_hubspot_contact_lifecycle": """
                SELECT
                    lifecyclestage,
                    hs_lead_status,
                    count() as contact_count,
                    countDistinct(company) as unique_companies
                FROM bronze_hubspot_contacts
                GROUP BY lifecyclestage, hs_lead_status
                ORDER BY contact_count DESC
            """,
            "v_hubspot_company_overview": """
                SELECT
                    id,
                    name,
                    domain,
                    industry,
                    annualrevenue,
                    numberofemployees,
                    type,
                    city,
                    state
                FROM bronze_hubspot_companies
                ORDER BY name
            """,
            "v_hubspot_engagement_summary": """
                SELECT
                    hs_engagement_type,
                    hs_engagement_source,
                    count() as engagement_count
                FROM bronze_hubspot_engagements
                GROUP BY hs_engagement_type, hs_engagement_source
                ORDER BY engagement_count DESC
            """,
            "v_hubspot_ticket_analysis": """
                SELECT
                    hs_ticket_priority,
                    hs_ticket_category,
                    hs_pipeline_stage,
                    count() as ticket_count
                FROM bronze_hubspot_tickets
                GROUP BY hs_ticket_priority, hs_ticket_category, hs_pipeline_stage
                ORDER BY ticket_count DESC
            """,
        }

        for view_name, sql in views.items():
            try:
                self.ch.create_view(view_name, sql)
                logger.info(f"[CH] View created: {view_name}")
            except Exception as e:
                logger.warning(f"[CH] View creation deferred: {view_name}", error=str(e))

    def get_table_stats(self) -> list[dict]:
        """Get row counts for all HubSpot tables."""
        stats = []
        for resource in HUBSPOT_SCHEMAS:
            table_name = f"bronze_hubspot_{resource}"
            try:
                if self.ch.table_exists(table_name):
                    count = self.ch.get_table_row_count(table_name)
                    stats.append({"table": table_name, "resource": resource, "rows": count})
            except Exception:
                stats.append({"table": table_name, "resource": resource, "rows": 0})
        return stats
