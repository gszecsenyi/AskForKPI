# Databricks Marketplace Integration Guide
## Building AskForKPI as a Databricks Native App

---

## Table of Contents
1. [Databricks Native App Overview](#databricks-native-app-overview)
2. [Connection Methods](#connection-methods)
3. [Authentication & Authorization](#authentication--authorization)
4. [Unity Catalog Integration](#unity-catalog-integration)
5. [SQL Warehouse Execution](#sql-warehouse-execution)
6. [App Framework & Deployment](#app-framework--deployment)
7. [Marketplace Requirements](#marketplace-requirements)
8. [Development Setup](#development-setup)
9. [Testing & Validation](#testing--validation)
10. [Go-Live Checklist](#go-live-checklist)

---

## Databricks Native App Overview

### What is a Databricks Native App?

A **Databricks Native App** is an application that:
- Runs inside the Databricks workspace
- Uses Databricks compute (clusters, SQL warehouses, jobs)
- Integrates with Unity Catalog for data access
- Leverages Databricks authentication
- Is installed and managed through Databricks Marketplace

### Key Benefits

| Feature | Benefit |
|---------|---------|
| **Native Integration** | No external setup, runs in customer workspace |
| **Security** | Data never leaves customer environment |
| **Compute** | Uses customer's existing Databricks compute |
| **Billing** | Rolls into customer's Databricks invoice |
| **Trust** | Pre-vetted by Databricks security team |

### Architecture Model

```
Customer's Databricks Workspace
    │
    ├─ AskForKPI App (your code)
    │   ├─ Backend (Python/FastAPI)
    │   ├─ Frontend (React)
    │   └─ Agent (LangGraph)
    │
    ├─ Unity Catalog (data & metadata)
    ├─ SQL Warehouse (compute)
    ├─ Volumes (file storage)
    └─ Databricks APIs (platform services)
```

---

## Connection Methods

### 1. Databricks REST API

**Base URL**: `https://<workspace-url>/api/2.1/`

**Authentication Options:**
- Personal Access Tokens (PAT)
- OAuth 2.0 (recommended for apps)
- Service Principal

#### Example: Basic Connection

```python
import requests
from databricks.sdk import WorkspaceClient

# Method 1: Using Databricks SDK (recommended)
client = WorkspaceClient(
    host="https://your-workspace.cloud.databricks.com",
    token="dapi..."  # or use OAuth
)

# Method 2: Direct REST API
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

response = requests.get(
    "https://your-workspace.cloud.databricks.com/api/2.1/clusters/list",
    headers=headers
)
```

### 2. Databricks SQL Connector

For executing SQL queries:

```python
from databricks import sql

# Connect to SQL Warehouse
connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123",
    access_token="dapi..."
)

cursor = connection.cursor()
cursor.execute("SELECT * FROM catalog.schema.table LIMIT 10")
result = cursor.fetchall()
cursor.close()
connection.close()
```

### 3. Unity Catalog API

For schema exploration and metadata:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

client = WorkspaceClient()

# List catalogs
catalogs = client.catalogs.list()
for catalog in catalogs:
    print(f"Catalog: {catalog.name}")

# List schemas in a catalog
schemas = client.schemas.list(catalog_name="main")
for schema in schemas:
    print(f"  Schema: {schema.name}")

# List tables in a schema
tables = client.tables.list(catalog_name="main", schema_name="default")
for table in tables:
    print(f"    Table: {table.name}")

# Get table details
table_info = client.tables.get(full_name="main.default.my_table")
print(f"Columns: {table_info.columns}")
```

### 4. Databricks File System (DBFS) & Volumes

For storing application state, configs, conversation history:

```python
# Using Volumes (recommended for Unity Catalog)
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()

# Create volume if not exists
try:
    client.volumes.create(
        catalog_name="main",
        schema_name="askforkpi",
        name="app_state",
        volume_type=VolumeType.MANAGED
    )
except Exception as e:
    print(f"Volume exists or error: {e}")

# Write to volume
volume_path = "/Volumes/main/askforkpi/app_state/conversations.json"
with open(volume_path, 'w') as f:
    json.dump(conversation_data, f)

# Read from volume
with open(volume_path, 'r') as f:
    data = json.load(f)
```

---

## Authentication & Authorization

### OAuth 2.0 Flow (Recommended)

Databricks supports OAuth 2.0 for Native Apps:

```python
# 1. Register your app with Databricks
# Get client_id and client_secret from Databricks Partner Portal

# 2. Implement OAuth flow
from requests_oauthlib import OAuth2Session

client_id = "your-client-id"
client_secret = "your-client-secret"
authorization_base_url = "https://your-workspace.cloud.databricks.com/oidc/v1/authorize"
token_url = "https://your-workspace.cloud.databricks.com/oidc/v1/token"
redirect_uri = "https://your-app.com/callback"

# Step 1: Redirect user to authorization URL
oauth = OAuth2Session(client_id, redirect_uri=redirect_uri)
authorization_url, state = oauth.authorization_url(authorization_base_url)
print(f"Please go to {authorization_url} and authorize access.")

# Step 2: Get the authorization code from callback
# (user is redirected back with code)

# Step 3: Fetch token
token = oauth.fetch_token(
    token_url,
    client_secret=client_secret,
    authorization_response=callback_url
)

# Step 4: Use token for API calls
access_token = token['access_token']
```

### Service Principal (for background jobs)

```python
from databricks.sdk import WorkspaceClient

# Create service principal via Databricks UI or API
# Grant necessary permissions in Unity Catalog

client = WorkspaceClient(
    host="https://your-workspace.cloud.databricks.com",
    client_id="service-principal-app-id",
    client_secret="service-principal-secret"
)

# Service principal can now perform operations
# based on granted permissions
```

### Permission Model

Your app needs these Unity Catalog permissions:

```sql
-- Grant permissions to service principal
GRANT USE CATALOG ON CATALOG main TO `service-principal-app-id`;
GRANT USE SCHEMA ON SCHEMA main.askforkpi TO `service-principal-app-id`;
GRANT CREATE TABLE ON SCHEMA main.askforkpi TO `service-principal-app-id`;
GRANT SELECT, MODIFY ON TABLE main.askforkpi.* TO `service-principal-app-id`;
```

---

## Unity Catalog Integration

### Reading Source Schemas

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

def get_source_tables(client: WorkspaceClient, catalog: str, schema: str):
    """
    Get all tables from a Unity Catalog schema
    Returns tables with column information
    """
    tables = client.tables.list(catalog_name=catalog, schema_name=schema)

    result = []
    for table in tables:
        table_info = client.tables.get(full_name=f"{catalog}.{schema}.{table.name}")

        columns = []
        for col in table_info.columns:
            columns.append({
                "name": col.name,
                "data_type": col.type_name.value,
                "comment": col.comment or "",
                "nullable": col.nullable
            })

        result.append({
            "name": table.name,
            "full_name": table_info.full_name,
            "table_type": table_info.table_type.value,
            "columns": columns,
            "comment": table_info.comment or ""
        })

    return result

# Usage
client = WorkspaceClient()
source_tables = get_source_tables(client, "main", "source_data")
```

### Creating Tables (Stage, Dimension, Fact)

```python
def create_dimensional_table(
    client: WorkspaceClient,
    catalog: str,
    schema: str,
    table_name: str,
    columns: list,
    table_type: str = "MANAGED"
):
    """
    Create a Delta table in Unity Catalog
    """
    # Build column definitions
    column_defs = []
    for col in columns:
        col_def = f"`{col['name']}` {col['data_type']}"
        if col.get('comment'):
            col_def += f" COMMENT '{col['comment']}'"
        column_defs.append(col_def)

    # Build CREATE TABLE statement
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
        {', '.join(column_defs)}
    )
    USING DELTA
    COMMENT 'Generated by AskForKPI'
    """

    # Execute via SQL Warehouse
    execute_sql(client, ddl)

    return f"{catalog}.{schema}.{table_name}"

# Example: Create dim_customer table
columns = [
    {"name": "id", "data_type": "BIGINT", "comment": "Surrogate key"},
    {"name": "customer_id", "data_type": "STRING", "comment": "Natural key"},
    {"name": "name", "data_type": "STRING", "comment": "Customer name"},
    {"name": "email", "data_type": "STRING", "comment": "Email address"},
    {"name": "created_at", "data_type": "TIMESTAMP", "comment": "Record created"},
]

create_dimensional_table(
    client,
    catalog="main",
    schema="dimensions",
    table_name="dim_customer",
    columns=columns
)
```

### Data Lineage

Unity Catalog automatically tracks lineage. Access it via API:

```python
def get_table_lineage(client: WorkspaceClient, table_full_name: str):
    """
    Get lineage for a table
    Shows upstream and downstream dependencies
    """
    lineage = client.lineage.get_lineage_by_table(
        table_name=table_full_name
    )

    return {
        "upstream": [up.name for up in lineage.upstreams or []],
        "downstream": [down.name for down in lineage.downstreams or []]
    }

# Usage
lineage = get_table_lineage(client, "main.dimensions.dim_customer")
print(f"Upstream: {lineage['upstream']}")
print(f"Downstream: {lineage['downstream']}")
```

---

## SQL Warehouse Execution

### Executing SQL Queries

```python
from databricks import sql
import os

def execute_sql(warehouse_id: str, query: str):
    """
    Execute SQL on Databricks SQL Warehouse
    """
    with sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=os.getenv("DATABRICKS_TOKEN")
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query)

            # Fetch results if SELECT
            if query.strip().upper().startswith("SELECT"):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return {"columns": columns, "rows": rows}
            else:
                return {"status": "success"}

# Usage
result = execute_sql(
    warehouse_id="abc123",
    query="SELECT * FROM main.dimensions.dim_customer LIMIT 10"
)
```

### Async Query Execution (for long-running queries)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

def execute_sql_async(client: WorkspaceClient, warehouse_id: str, query: str):
    """
    Execute SQL asynchronously and poll for results
    """
    # Start query execution
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="0s"  # Don't wait, return immediately
    )

    statement_id = response.statement_id

    # Poll for completion
    while True:
        status = client.statement_execution.get_statement(statement_id)

        if status.status.state == StatementState.SUCCEEDED:
            return status.result
        elif status.status.state in [StatementState.FAILED, StatementState.CANCELED]:
            raise Exception(f"Query failed: {status.status.error}")

        time.sleep(1)  # Wait before polling again

# Usage in background task
from celery import Celery

@celery_app.task
def execute_ddl_async(warehouse_id, ddl_statements):
    client = WorkspaceClient()
    results = []

    for ddl in ddl_statements:
        result = execute_sql_async(client, warehouse_id, ddl)
        results.append(result)

    return results
```

---

## App Framework & Deployment

### Databricks App Framework Structure

```
askforkpi-app/
├── databricks.yml           # App manifest
├── app/
│   ├── __init__.py
│   ├── main.py             # FastAPI app
│   ├── config.py           # App configuration
│   └── agent/              # LangGraph agent
│       ├── __init__.py
│       ├── graph.py
│       └── tools.py
├── frontend/
│   ├── src/
│   │   ├── App.tsx
│   │   └── components/
│   └── package.json
├── requirements.txt
└── README.md
```

### databricks.yml (App Manifest)

```yaml
name: askforkpi
display_name: "AskForKPI - AI Dimensional Modeling"
version: "1.0.0"
description: |
  AI-powered dimensional modeling assistant that automatically designs
  and creates KPIs, dimension tables, and fact tables using natural language.

author: "Your Company"
license: "Proprietary"
min_databricks_version: "13.0"

# Permissions required
permissions:
  - unity_catalog:
      - READ_METADATA
      - CREATE_TABLE
      - WRITE_DATA
  - sql_warehouse:
      - EXECUTE_QUERY
  - volumes:
      - READ_WRITE

# App entry point
entrypoint:
  type: "webapp"
  module: "app.main:app"
  port: 8000

# Resources
compute:
  warehouse_id: "${var.warehouse_id}"  # User configures

# Installation
install:
  - create_catalog: true
    catalog_name: "askforkpi"
  - create_schema: true
    schema_name: "metadata"
  - create_volume: true
    volume_name: "app_state"

# Configuration
config:
  - name: "OPENAI_API_KEY"
    type: "secret"
    required: true
    description: "OpenAI API key for LLM"
  - name: "warehouse_id"
    type: "string"
    required: true
    description: "SQL Warehouse ID for query execution"
```

### FastAPI Backend for Databricks

```python
# app/main.py
from fastapi import FastAPI, Depends, HTTPException
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import os

# Initialize FastAPI
app = FastAPI(
    title="AskForKPI",
    description="AI-powered dimensional modeling for Databricks",
    version="1.0.0"
)

# Databricks client singleton
def get_databricks_client():
    """
    Get authenticated Databricks client
    Uses workspace identity when running in Databricks
    """
    return WorkspaceClient()

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "1.0.0"}

@app.get("/api/v1/catalogs")
def list_catalogs(client: WorkspaceClient = Depends(get_databricks_client)):
    """List available Unity Catalog catalogs"""
    try:
        catalogs = client.catalogs.list()
        return [{"name": c.name, "comment": c.comment} for c in catalogs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/chat")
async def chat(
    message: str,
    project_id: str,
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """
    Chat endpoint - calls LangGraph agent
    """
    from app.agent.graph import run_agent

    result = await run_agent(
        message=message,
        project_id=project_id,
        client=client
    )

    return result

# Mount frontend (React build)
from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="frontend/build", html=True), name="frontend")
```

### Deployment via Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Deploy app
databricks apps create \
  --name askforkpi \
  --source-directory ./askforkpi-app \
  --warehouse-id abc123

# Update existing app
databricks apps update --app-id askforkpi-v1 \
  --source-directory ./askforkpi-app

# Check status
databricks apps get --app-id askforkpi-v1
```

---

## Marketplace Requirements

### 1. Security Requirements

**Must Have:**
- ✓ SOC 2 Type II certification
- ✓ Penetration testing report (within 12 months)
- ✓ Vulnerability scanning
- ✓ Data encryption at rest and in transit
- ✓ No external data exfiltration

**Security Checklist:**
```python
# Example: Ensure data stays in workspace
class SecurityValidator:
    @staticmethod
    def validate_no_external_calls(func):
        """
        Decorator to ensure function doesn't call external APIs
        (except approved LLM providers)
        """
        approved_domains = [
            "api.openai.com",
            "*.databricks.com"
        ]

        def wrapper(*args, **kwargs):
            # Monitor network calls
            with NetworkMonitor(approved_domains) as monitor:
                result = func(*args, **kwargs)

                if monitor.unauthorized_calls:
                    raise SecurityError("Unauthorized external API call detected")

                return result

        return wrapper
```

### 2. Compliance Requirements

- GDPR compliant (EU data stays in EU)
- HIPAA compliant (for healthcare customers)
- SOX compliant (audit logs)
- CCPA compliant (California privacy)

### 3. Performance Requirements

| Metric | Requirement |
|--------|-------------|
| App startup time | < 10 seconds |
| API response time (p95) | < 1 second |
| Table creation | < 5 seconds |
| Concurrent users | 100+ per workspace |
| Query timeout | < 30 seconds |

### 4. Documentation Requirements

- [ ] Architecture diagram
- [ ] Installation guide
- [ ] User documentation
- [ ] API documentation
- [ ] Troubleshooting guide
- [ ] Video demo (3-5 minutes)
- [ ] Case studies (2-3 customers)

### 5. Support Requirements

- **Response SLA**:
  - Critical: 4 hours
  - High: 1 business day
  - Medium: 3 business days
  - Low: 5 business days

- **Support Channels**:
  - Email: support@askforkpi.com
  - Slack Connect (enterprise customers)
  - Databricks Partner Support Portal

---

## Development Setup

### Local Development Environment

```bash
# 1. Clone repository
git clone https://github.com/yourcompany/askforkpi
cd askforkpi

# 2. Set up Python environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure Databricks connection
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="abc123"

# 4. Set up OpenAI (for LLM)
export OPENAI_API_KEY="sk-..."

# 5. Run locally (connects to Databricks)
python -m uvicorn app.main:app --reload --port 8000
```

### Testing with Real Databricks Workspace

```python
# tests/test_integration.py
import pytest
from databricks.sdk import WorkspaceClient
from app.agent.tools import create_dimensional_table

@pytest.fixture
def databricks_client():
    """Real Databricks client for integration tests"""
    return WorkspaceClient()

def test_create_table_in_databricks(databricks_client):
    """Test table creation in real Databricks workspace"""
    # Use test catalog/schema
    catalog = "test_askforkpi"
    schema = "integration_tests"
    table_name = f"test_table_{int(time.time())}"

    # Create table
    full_name = create_dimensional_table(
        client=databricks_client,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        columns=[
            {"name": "id", "data_type": "BIGINT"},
            {"name": "name", "data_type": "STRING"}
        ]
    )

    # Verify table exists
    table_info = databricks_client.tables.get(full_name=full_name)
    assert table_info is not None
    assert len(table_info.columns) == 2

    # Cleanup
    databricks_client.tables.delete(full_name=full_name)
```

---

## Testing & Validation

### Pre-Marketplace Testing Checklist

**Functionality:**
- [ ] App installs successfully in test workspace
- [ ] OAuth authentication works
- [ ] Unity Catalog read/write operations work
- [ ] SQL Warehouse queries execute correctly
- [ ] LangGraph agent generates valid tables
- [ ] DDL execution creates tables successfully
- [ ] Frontend loads in Databricks UI
- [ ] All API endpoints return expected responses

**Performance:**
- [ ] Load test with 50 concurrent users
- [ ] Large schema (1000+ tables) performance
- [ ] Query execution under 5 seconds
- [ ] App memory usage < 2GB

**Security:**
- [ ] Penetration test passed
- [ ] No external data leakage
- [ ] Secrets encrypted
- [ ] Audit logging functional

**Usability:**
- [ ] 5 beta users tested successfully
- [ ] Average session duration > 10 minutes
- [ ] User feedback NPS > 40
- [ ] Zero critical bugs

---

## Go-Live Checklist

### Phase 1: Pre-Submission (2-4 weeks before)

- [ ] Complete SOC 2 audit
- [ ] Penetration testing report
- [ ] Create demo video
- [ ] Write case studies (3 customers)
- [ ] Prepare marketing materials
- [ ] Set up support infrastructure

### Phase 2: Marketplace Submission

```markdown
# Marketplace Listing Content

## Title
AskForKPI - AI-Powered Dimensional Modeling for Databricks

## Short Description (160 chars)
Generate KPIs and dimensional models in minutes using AI. Built for Unity Catalog and Delta Lake.

## Long Description
AskForKPI is an AI-powered dimensional modeling assistant that helps data teams:

- Design star schemas and dimensional models through natural language
- Automatically generate stage, dimension, and fact tables
- Create production-ready Delta tables in Unity Catalog
- Follow Kimball methodology best practices
- Reduce modeling time from weeks to minutes

Perfect for:
- Data engineers overwhelmed with KPI requests
- Analysts who need dimensional models without SQL expertise
- Teams adopting Unity Catalog and data mesh patterns

Built exclusively for Databricks with deep integration into Unity Catalog, SQL Warehouses, and Delta Lake.

## Category
Data Engineering, Analytics, AI/ML

## Pricing
Starting at $995/month for teams
Enterprise pricing available

## Support
Email, Slack, Dedicated CSM (Enterprise)
```

### Phase 3: Review Process (4-6 weeks)

**What Databricks Reviews:**
1. Security assessment
2. Code quality review
3. Performance testing
4. Documentation completeness
5. Support capability
6. Customer references

**Your Responsibilities During Review:**
- Respond to questions within 24 hours
- Provide additional documentation as requested
- Fix any issues identified
- Demo app to Databricks Partner Engineering

### Phase 4: Launch (1-2 weeks)

- [ ] Marketplace listing goes live
- [ ] Co-marketing announcement with Databricks
- [ ] Blog post on Databricks blog
- [ ] Social media campaign
- [ ] Email to design partners
- [ ] Monitor first customers closely

---

## Resources & Links

### Official Documentation
- [Databricks App Framework](https://docs.databricks.com/dev-tools/app-framework.html)
- [Unity Catalog API](https://docs.databricks.com/api/workspace/catalogs)
- [SQL Warehouse API](https://docs.databricks.com/sql/api/index.html)
- [Databricks SDK for Python](https://docs.databricks.com/dev-tools/sdk-python.html)

### Partner Program
- [Databricks Technology Partner Program](https://www.databricks.com/partners/technology)
- [Partner Portal](https://partners.databricks.com)
- Partner Support: partners@databricks.com

### Community
- [Databricks Community Forums](https://community.databricks.com)
- [Data + AI Summit](https://www.databricks.com/dataaisummit/)
- [Databricks Slack (request access)](https://databricks.com/slack)

### Example Apps
- Study other marketplace apps
- Review "Built on Databricks" badge requirements
- Join partner webinars

---

## FAQ

**Q: How long does marketplace approval take?**
A: 6-12 weeks typically, but can be expedited with good preparation.

**Q: Can I charge for my app?**
A: Yes. Databricks supports subscription and consumption-based pricing.

**Q: What if Databricks builds similar functionality?**
A: As a marketplace partner, you'll have early visibility. Most successful outcomes are acquisitions.

**Q: Do I need to be SOC 2 certified?**
A: Yes, for enterprise customers. Start process early (3-6 months).

**Q: Can I offer a free tier?**
A: Yes, many apps have free trials or limited free tiers.

**Q: How does billing work?**
A: Databricks handles billing and remits payment to you (typically 70-80% after marketplace fee).

---

**Last Updated**: 2025-12-19
**Version**: 1.0.0
**Status**: Ready for Development
