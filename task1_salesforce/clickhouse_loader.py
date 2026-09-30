"""Salesforce ClickHouse Loader.

Dynamically creates ClickHouse tables from Salesforce object schemas
and inserts extracted records with proper data typing and partitioning.
"""

import structlog
from typing import Optional

from shared.clickhouse_client import get_clickhouse_client, PY_TO_CH_TYPE

logger = structlog.get_logger(__name__)

# Schema definitions for Salesforce objects → ClickHouse column types
SF_OBJECT_SCHEMAS = {
    "Account": {
        "Id": "String", "Name": "String", "Industry": "String",
        "AnnualRevenue": "Float64", "NumberOfEmployees": "Int32",
        "BillingCity": "String", "BillingState": "String", "BillingCountry": "String",
        "Phone": "String", "Website": "String", "Type": "String",
        "CreatedDate": "String", "LastModifiedDate": "String",
    },
    "Contact": {
        "Id": "String", "FirstName": "String", "LastName": "String",
        "Email": "String", "Phone": "String", "Title": "String",
        "Department": "String", "AccountId": "String",
        "MailingCity": "String", "MailingState": "String",
        "LeadSource": "String", "CreatedDate": "String", "LastModifiedDate": "String",
    },
    "Opportunity": {
        "Id": "String", "Name": "String", "StageName": "String",
        "Amount": "Float64", "CloseDate": "String", "Probability": "Int32",
        "AccountId": "String", "Type": "String", "LeadSource": "String",
        "ForecastCategory": "String", "IsClosed": "UInt8", "IsWon": "UInt8",
        "CreatedDate": "String",
    },
    "Lead": {
        "Id": "String", "FirstName": "String", "LastName": "String",
        "Email": "String", "Company": "String", "Title": "String",
        "Status": "String", "LeadSource": "String", "Industry": "String",
        "Rating": "String", "Phone": "String", "City": "String",
        "State": "String", "CreatedDate": "String",
    },
    "Task": {
        "Id": "String", "Subject": "String", "Status": "String",
        "Priority": "String", "WhoId": "String", "WhatId": "String",
        "ActivityDate": "String", "Description": "String",
        "OwnerId": "String", "IsHighPriority": "UInt8", "CreatedDate": "String",
    },
    "Case": {
        "Id": "String", "Subject": "String", "Status": "String",
        "Priority": "String", "Origin": "String", "Type": "String",
        "Reason": "String", "AccountId": "String", "ContactId": "String",
        "Description": "String", "IsClosed": "UInt8", "CreatedDate": "String",
    },
    "Product2": {
        "Id": "String", "Name": "String", "ProductCode": "String",
        "Description": "String", "Family": "String", "IsActive": "UInt8",
        "QuantityUnitOfMeasure": "String", "StockKeepingUnit": "String",
        "CreatedDate": "String",
    },
    "PricebookEntry": {
        "Id": "String", "Name": "String", "Pricebook2Id": "String",
        "Product2Id": "String", "UnitPrice": "Float64", "IsActive": "UInt8",
        "UseStandardPrice": "UInt8", "CurrencyIsoCode": "String",
        "CreatedDate": "String",
    },
    "Contract": {
        "Id": "String", "AccountId": "String", "Status": "String",
        "StartDate": "String", "EndDate": "String", "ContractTerm": "Int32",
        "OwnerExpirationNotice": "String", "ContractNumber": "String",
        "Description": "String", "CreatedDate": "String",
    },
    "Asset": {
        "Id": "String", "Name": "String", "AccountId": "String",
        "ContactId": "String", "Product2Id": "String", "SerialNumber": "String",
        "Status": "String", "InstallDate": "String", "PurchaseDate": "String",
        "Quantity": "Int32", "Price": "Float64", "CreatedDate": "String",
    },
    "Campaign": {
        "Id": "String", "Name": "String", "Type": "String",
        "Status": "String", "StartDate": "String", "EndDate": "String",
        "BudgetedCost": "Float64", "ActualCost": "Float64",
        "ExpectedRevenue": "Float64", "NumberSent": "Int32",
        "NumberOfLeads": "Int32", "NumberOfContacts": "Int32",
        "CreatedDate": "String",
    },
    "Event": {
        "Id": "String", "Subject": "String", "StartDateTime": "String",
        "EndDateTime": "String", "Location": "String", "WhoId": "String",
        "WhatId": "String", "Description": "String",
        "IsAllDayEvent": "UInt8", "OwnerId": "String", "CreatedDate": "String",
    },
}


class SalesforceClickHouseLoader:
    """Manages ClickHouse tables for Salesforce objects."""

    def __init__(self):
        self.ch = get_clickhouse_client()

    def create_table_for_object(
        self,
        object_name: str,
        sample_records: list[dict] = None,
    ) -> str:
        """Create a ClickHouse table for a Salesforce object.

        Uses the predefined schema if available, otherwise infers from sample records.
        """
        table_name = f"bronze_salesforce_{object_name.lower()}"

        # Use predefined schema
        schema = SF_OBJECT_SCHEMAS.get(object_name)
        if not schema and sample_records:
            # Auto-infer schema from records
            schema = {}
            for key, value in sample_records[0].items():
                if isinstance(value, bool):
                    schema[key] = "UInt8"
                elif isinstance(value, int):
                    schema[key] = "Int64"
                elif isinstance(value, float):
                    schema[key] = "Float64"
                else:
                    schema[key] = "String"

        if not schema:
            raise ValueError(f"No schema available for {object_name}")

        ddl = self.ch.create_table(
            table_name=table_name,
            columns=schema,
            order_by=["Id"],
        )

        logger.info(f"[CH] Table created/verified: {table_name}", columns=len(schema))
        return ddl

    def insert_records(self, table_name: str, records: list[dict]):
        """Insert records into a ClickHouse table, handling type coercion."""
        if not records:
            return

        # Get the expected columns for the table
        schema = None
        for obj_name, obj_schema in SF_OBJECT_SCHEMAS.items():
            if table_name == f"bronze_salesforce_{obj_name.lower()}":
                schema = obj_schema
                break

        if schema:
            columns = list(schema.keys())
            # Coerce values to match schema
            coerced = []
            for record in records:
                row = {}
                for col in columns:
                    val = record.get(col)
                    expected_type = schema[col]

                    if val is None:
                        row[col] = val
                    elif expected_type == "UInt8":
                        row[col] = 1 if val in (True, "true", "True", 1) else 0
                    elif expected_type in ("Int32", "Int64"):
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
                        row[col] = str(val) if val is not None else None
                coerced.append(row)

            self.ch.insert_data(table_name, coerced, columns)
        else:
            self.ch.insert_data(table_name, records)

    def create_all_tables(self):
        """Create ClickHouse tables for all 12 Salesforce objects."""
        for object_name in SF_OBJECT_SCHEMAS:
            self.create_table_for_object(object_name)
        logger.info(f"[CH] All {len(SF_OBJECT_SCHEMAS)} Salesforce tables created")

    def create_analytical_views(self):
        """Create ClickHouse analytical views across Salesforce objects."""
        views = {
            "v_salesforce_pipeline_summary": """
                SELECT
                    StageName,
                    count() as deal_count,
                    sum(Amount) as total_amount,
                    avg(Amount) as avg_amount,
                    avg(Probability) as avg_probability
                FROM bronze_salesforce_opportunity
                GROUP BY StageName
                ORDER BY total_amount DESC
            """,
            "v_salesforce_account_overview": """
                SELECT
                    a.Id as account_id,
                    a.Name as account_name,
                    a.Industry,
                    a.AnnualRevenue,
                    a.NumberOfEmployees,
                    a.Type as account_type
                FROM bronze_salesforce_account a
                ORDER BY a.AnnualRevenue DESC
            """,
            "v_salesforce_lead_funnel": """
                SELECT
                    Status,
                    LeadSource,
                    Rating,
                    count() as lead_count
                FROM bronze_salesforce_lead
                GROUP BY Status, LeadSource, Rating
                ORDER BY lead_count DESC
            """,
            "v_salesforce_case_analysis": """
                SELECT
                    Status,
                    Priority,
                    Origin,
                    Type,
                    count() as case_count
                FROM bronze_salesforce_case
                GROUP BY Status, Priority, Origin, Type
                ORDER BY case_count DESC
            """,
        }

        for view_name, sql in views.items():
            try:
                self.ch.create_view(view_name, sql)
                logger.info(f"[CH] View created: {view_name}")
            except Exception as e:
                logger.warning(f"[CH] View creation deferred: {view_name}", error=str(e))

    def get_table_stats(self) -> list[dict]:
        """Get row counts for all Salesforce tables."""
        stats = []
        for object_name in SF_OBJECT_SCHEMAS:
            table_name = f"bronze_salesforce_{object_name.lower()}"
            try:
                if self.ch.table_exists(table_name):
                    count = self.ch.get_table_row_count(table_name)
                    stats.append({"table": table_name, "object": object_name, "rows": count})
            except Exception:
                stats.append({"table": table_name, "object": object_name, "rows": 0})
        return stats
