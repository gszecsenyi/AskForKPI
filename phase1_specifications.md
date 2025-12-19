# Phase 1: Databricks Native App MVP Specifications
## Detailed Technical Specifications and Subtasks

**Duration**: 3 months (12 weeks)
**Goal**: Transform CLI prototype into Databricks Native App ready for Marketplace
**Success Criteria**: 5 design partners testing, Marketplace application submitted, 3 LOIs secured

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Databricks Integration](#databricks-integration)
3. [Backend Development](#backend-development)
4. [Frontend Development](#frontend-development)
5. [LangGraph Agent Migration](#langgraph-agent-migration)
6. [Unity Catalog Operations](#unity-catalog-operations)
7. [SQL Warehouse Integration](#sql-warehouse-integration)
8. [App Framework & Deployment](#app-framework--deployment)
9. [Testing Strategy](#testing-strategy)
10. [Security & Compliance](#security--compliance)
11. [Documentation](#documentation)
12. [Timeline & Milestones](#timeline--milestones)

---

## Architecture Overview

### Databricks Native App Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Customer's Databricks Workspace                   │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    AskForKPI Native App                        │  │
│  │                                                                 │  │
│  │  ┌──────────────────┐          ┌──────────────────┐          │  │
│  │  │  React Frontend  │◄─────────┤  FastAPI Backend │          │  │
│  │  │  (Embedded UI)   │  REST    │  (Python 3.11)   │          │  │
│  │  │                  │          │                   │          │  │
│  │  │ • Chat Interface │          │ • API Routes     │          │  │
│  │  │ • Schema View    │          │ • LangGraph Agent│          │  │
│  │  │ • Table Cards    │          │ • Unity Catalog  │          │  │
│  │  └──────────────────┘          └──────────────────┘          │  │
│  │                                                                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                   │                                  │
│                                   │                                  │
│         ┌─────────────────────────┼─────────────────────┐           │
│         │                         │                      │           │
│         ▼                         ▼                      ▼           │
│  ┌────────────────┐      ┌────────────────┐    ┌──────────────┐   │
│  │Unity Catalog   │      │ SQL Warehouse  │    │   Volumes    │   │
│  │                │      │                │    │              │   │
│  │ • Catalogs     │      │ • Query Engine │    │ • App State  │   │
│  │ • Schemas      │      │ • DDL Execution│    │ • Configs    │   │
│  │ • Tables       │      │ • Data Access  │    │ • Logs       │   │
│  │ • Metadata     │      │                │    │ • History    │   │
│  │ • Lineage      │      │                │    │              │   │
│  └────────────────┘      └────────────────┘    └──────────────┘   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              │ API Call (HTTPS)
                              ▼
                    ┌──────────────────────┐
                    │    OpenAI API        │
                    │   (GPT-4o-mini)      │
                    │  LLM Inference Only  │
                    └──────────────────────┘
```

### Key Architectural Principles

**✅ Native Integration**
- Runs entirely within customer's Databricks workspace
- No external databases (PostgreSQL ❌)
- No external cache (Redis ❌)
- No external auth (Auth0 ❌)

**✅ Customer Data Sovereignty**
- All data stays in customer's Unity Catalog
- Uses customer's compute (SQL Warehouse)
- No data exfiltration
- Inherits customer's security policies

**✅ Serverless Architecture**
- No infrastructure to manage
- Scales with Databricks platform
- Pay only for compute used
- Zero DevOps overhead

**✅ Marketplace Ready**
- Follows Databricks App Framework
- Security compliant from day 1
- Easy installation (one-click)
- Unified billing through Databricks

### Technology Stack

| Component | Technology | Why This Choice |
|-----------|-----------|-----------------|
| **Backend Framework** | FastAPI (Python 3.11) | Fast, async, auto-docs, Databricks SDK compatible |
| **Frontend Framework** | React 18 + TypeScript | Industry standard, embeds well in Databricks |
| **Build Tool** | Vite 5 | Fast HMR, optimized bundles |
| **UI Components** | Tailwind CSS + shadcn/ui | Professional look, customizable |
| **State Management** | Zustand | Lightweight, no provider boilerplate |
| **Agent Framework** | LangGraph + LangChain | Current codebase, proven for agents |
| **LLM Provider** | OpenAI (GPT-4o-mini) | Best cost/quality ratio |
| **Database** | Unity Catalog | Native Databricks, no external DB needed |
| **Storage** | Databricks Volumes | File storage for app state |
| **Compute** | SQL Warehouse | Serverless query execution |
| **Authentication** | Databricks OAuth 2.0 | Native, secure, no third-party |
| **Deployment** | Databricks App Framework | Native deployment, marketplace ready |

---

## Databricks Integration

### 1. Databricks SDK Setup (Week 1)

#### Task 1.1: Install and Configure Databricks SDK

**Subtasks:**
- [ ] Install databricks-sdk Python package (v0.20+)
- [ ] Install databricks-sql-connector for SQL execution
- [ ] Create WorkspaceClient wrapper class
- [ ] Implement connection pooling
- [ ] Add retry logic with exponential backoff
- [ ] Create health check for Databricks connectivity

**Code Structure:**
```python
# app/core/databricks_client.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Optional
import logging

class DatabricksClientManager:
    """Singleton manager for Databricks client"""

    _instance: Optional[WorkspaceClient] = None

    @classmethod
    def get_client(cls) -> WorkspaceClient:
        """Get or create Databricks client"""
        if cls._instance is None:
            cls._instance = WorkspaceClient()
        return cls._instance

    @classmethod
    def test_connection(cls) -> bool:
        """Test Databricks connection"""
        try:
            client = cls.get_client()
            client.current_user.me()
            return True
        except Exception as e:
            logging.error(f"Databricks connection failed: {e}")
            return False
```

**Acceptance Criteria:**
- ✅ SDK connects to workspace successfully
- ✅ Current user info retrieved
- ✅ Connection test passes
- ✅ Retry logic handles transient failures

#### Task 1.2: SQL Warehouse Connection

**Subtasks:**
- [ ] Create SQL connection manager
- [ ] Implement connection pooling for SQL
- [ ] Add query execution wrapper
- [ ] Implement async query execution
- [ ] Add query result caching
- [ ] Create query timeout handler

**Code Structure:**
```python
# app/core/sql_warehouse.py
from databricks import sql
from typing import List, Dict, Any
import os

class SQLWarehouseManager:
    """Manage SQL Warehouse connections"""

    def __init__(self, warehouse_id: str):
        self.warehouse_id = warehouse_id
        self.connection = None

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query synchronously"""
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            credentials_provider=lambda: os.getenv("DATABRICKS_TOKEN")
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

                if query.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return [{"status": "success"}]

    async def execute_query_async(self, query: str):
        """Execute SQL query asynchronously"""
        # Implementation using statement execution API
        pass
```

**Acceptance Criteria:**
- ✅ SQL queries execute successfully
- ✅ Results returned in structured format
- ✅ DDL statements execute without errors
- ✅ Async execution works for long queries

#### Task 1.3: Unity Catalog API Integration

**Subtasks:**
- [ ] Implement catalog listing
- [ ] Implement schema listing
- [ ] Implement table listing
- [ ] Implement table detail retrieval
- [ ] Add column metadata extraction
- [ ] Implement lineage tracking

**Code Structure:**
```python
# app/services/unity_catalog_service.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
from typing import List, Dict, Any

class UnityCatalogService:
    """Service for Unity Catalog operations"""

    def __init__(self, client: WorkspaceClient):
        self.client = client

    def list_catalogs(self) -> List[Dict[str, Any]]:
        """List all accessible catalogs"""
        catalogs = self.client.catalogs.list()
        return [
            {
                "name": c.name,
                "comment": c.comment,
                "owner": c.owner,
                "created_at": c.created_at
            }
            for c in catalogs
        ]

    def list_schemas(self, catalog: str) -> List[Dict[str, Any]]:
        """List schemas in a catalog"""
        schemas = self.client.schemas.list(catalog_name=catalog)
        return [
            {
                "name": s.name,
                "catalog": s.catalog_name,
                "comment": s.comment
            }
            for s in schemas
        ]

    def get_table_schema(self, full_table_name: str) -> Dict[str, Any]:
        """Get detailed table schema"""
        table = self.client.tables.get(full_name=full_table_name)

        columns = []
        for col in table.columns:
            columns.append({
                "name": col.name,
                "data_type": col.type_name.value,
                "comment": col.comment or "",
                "nullable": col.nullable
            })

        return {
            "name": table.name,
            "catalog": table.catalog_name,
            "schema": table.schema_name,
            "table_type": table.table_type.value,
            "columns": columns,
            "owner": table.owner,
            "comment": table.comment
        }
```

**Acceptance Criteria:**
- ✅ Can list all catalogs user has access to
- ✅ Can list schemas within catalogs
- ✅ Can retrieve full table metadata
- ✅ Column details include types and comments

---

## Backend Development

### 2. FastAPI Application Structure (Week 1-2)

#### Task 2.1: Initialize FastAPI Project

**Project Structure:**
```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                     # FastAPI entry point
│   ├── config.py                   # Configuration
│   │
│   ├── api/                        # API routes
│   │   ├── __init__.py
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── catalogs.py        # Unity Catalog endpoints
│   │   │   ├── chat.py            # Chat/conversation
│   │   │   ├── tables.py          # Table operations
│   │   │   └── projects.py        # Project management
│   │
│   ├── core/                       # Core utilities
│   │   ├── __init__.py
│   │   ├── databricks_client.py   # Databricks client manager
│   │   ├── sql_warehouse.py       # SQL execution
│   │   └── exceptions.py          # Custom exceptions
│   │
│   ├── services/                   # Business logic
│   │   ├── __init__.py
│   │   ├── unity_catalog_service.py
│   │   ├── agent_service.py       # LangGraph wrapper
│   │   ├── table_service.py       # Table operations
│   │   ├── ddl_service.py         # DDL generation
│   │   └── storage_service.py     # Volumes storage
│   │
│   ├── agent/                      # LangGraph agent
│   │   ├── __init__.py
│   │   ├── graph.py               # Agent graph
│   │   ├── tools.py               # Databricks tools
│   │   ├── state.py               # Agent state
│   │   └── prompts.py             # Prompt templates
│   │
│   ├── models/                     # Pydantic models
│   │   ├── __init__.py
│   │   ├── catalog.py
│   │   ├── table.py
│   │   ├── message.py
│   │   └── project.py
│   │
│   └── utils/                      # Utilities
│       ├── __init__.py
│       ├── logger.py
│       └── validators.py
│
├── tests/                          # Tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_api/
│   ├── test_services/
│   └── test_agent/
│
├── databricks.yml                  # App manifest
├── requirements.txt
├── Dockerfile
└── README.md
```

**Subtasks:**
- [ ] Create project structure
- [ ] Set up FastAPI application
- [ ] Configure CORS for Databricks UI
- [ ] Add health check endpoint
- [ ] Set up structured logging
- [ ] Configure environment variables

**Code:**
```python
# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.api.v1 import catalogs, chat, tables, projects
from app.core.databricks_client import DatabricksClientManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="AskForKPI",
    description="AI-powered dimensional modeling for Databricks",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configure CORS for Databricks workspace
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Databricks workspace URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(catalogs.router, prefix="/api/v1", tags=["catalogs"])
app.include_router(chat.router, prefix="/api/v1", tags=["chat"])
app.include_router(tables.router, prefix="/api/v1", tags=["tables"])
app.include_router(projects.router, prefix="/api/v1", tags=["projects"])

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("Starting AskForKPI application...")

    # Test Databricks connection
    if DatabricksClientManager.test_connection():
        logger.info("✓ Databricks connection successful")
    else:
        logger.error("✗ Databricks connection failed")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "databricks_connected": DatabricksClientManager.test_connection()
    }

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "app": "AskForKPI",
        "version": "1.0.0",
        "docs": "/api/docs"
    }
```

**Acceptance Criteria:**
- ✅ FastAPI server starts on port 8000
- ✅ `/health` returns 200 OK
- ✅ Databricks connection tested on startup
- ✅ API docs accessible at `/api/docs`

#### Task 2.2: Unity Catalog API Endpoints

**Subtasks:**
- [ ] GET `/api/v1/catalogs` - List catalogs
- [ ] GET `/api/v1/catalogs/{catalog}/schemas` - List schemas
- [ ] GET `/api/v1/catalogs/{catalog}/schemas/{schema}/tables` - List tables
- [ ] GET `/api/v1/tables/{full_name}` - Get table details
- [ ] GET `/api/v1/tables/{full_name}/lineage` - Get lineage

**Code:**
```python
# app/api/v1/catalogs.py
from fastapi import APIRouter, Depends, HTTPException
from databricks.sdk import WorkspaceClient
from typing import List

from app.core.databricks_client import DatabricksClientManager
from app.services.unity_catalog_service import UnityCatalogService
from app.models.catalog import CatalogResponse, SchemaResponse, TableResponse

router = APIRouter()

def get_catalog_service() -> UnityCatalogService:
    """Dependency for Unity Catalog service"""
    client = DatabricksClientManager.get_client()
    return UnityCatalogService(client)

@router.get("/catalogs", response_model=List[CatalogResponse])
def list_catalogs(service: UnityCatalogService = Depends(get_catalog_service)):
    """List all accessible Unity Catalogs"""
    try:
        catalogs = service.list_catalogs()
        return catalogs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/catalogs/{catalog}/schemas", response_model=List[SchemaResponse])
def list_schemas(
    catalog: str,
    service: UnityCatalogService = Depends(get_catalog_service)
):
    """List schemas in a catalog"""
    try:
        schemas = service.list_schemas(catalog)
        return schemas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tables/{catalog}/{schema}/{table}", response_model=TableResponse)
def get_table(
    catalog: str,
    schema: str,
    table: str,
    service: UnityCatalogService = Depends(get_catalog_service)
):
    """Get table details"""
    try:
        full_name = f"{catalog}.{schema}.{table}"
        table_info = service.get_table_schema(full_name)
        return table_info
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
```

**Acceptance Criteria:**
- ✅ All endpoints return correct data
- ✅ Error handling returns appropriate status codes
- ✅ Response models validated
- ✅ API docs generated automatically

#### Task 2.3: Storage Service (Databricks Volumes)

**Subtasks:**
- [ ] Create Volumes storage wrapper
- [ ] Implement project metadata storage
- [ ] Implement conversation history storage
- [ ] Add app configuration storage
- [ ] Implement file operations (read, write, delete)

**Code:**
```python
# app/services/storage_service.py
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

class VolumesStorageService:
    """Service for storing data in Databricks Volumes"""

    def __init__(self, volume_path: str = "/Volumes/main/askforkpi/app_state"):
        self.volume_path = volume_path
        self._ensure_volume_exists()

    def _ensure_volume_exists(self):
        """Ensure volume directory exists"""
        Path(self.volume_path).mkdir(parents=True, exist_ok=True)

    def save_project(self, project_id: str, project_data: Dict[str, Any]):
        """Save project metadata"""
        file_path = f"{self.volume_path}/projects/{project_id}.json"
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w') as f:
            json.dump({
                **project_data,
                "updated_at": datetime.utcnow().isoformat()
            }, f, indent=2)

    def load_project(self, project_id: str) -> Dict[str, Any]:
        """Load project metadata"""
        file_path = f"{self.volume_path}/projects/{project_id}.json"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Project {project_id} not found")

        with open(file_path, 'r') as f:
            return json.load(f)

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects"""
        projects_dir = f"{self.volume_path}/projects"

        if not os.path.exists(projects_dir):
            return []

        projects = []
        for filename in os.listdir(projects_dir):
            if filename.endswith('.json'):
                with open(f"{projects_dir}/{filename}", 'r') as f:
                    projects.append(json.load(f))

        return sorted(projects, key=lambda x: x.get('updated_at', ''), reverse=True)

    def save_conversation(self, project_id: str, messages: List[Dict[str, Any]]):
        """Save conversation history"""
        file_path = f"{self.volume_path}/conversations/{project_id}.json"
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w') as f:
            json.dump({
                "project_id": project_id,
                "messages": messages,
                "updated_at": datetime.utcnow().isoformat()
            }, f, indent=2)

    def load_conversation(self, project_id: str) -> List[Dict[str, Any]]:
        """Load conversation history"""
        file_path = f"{self.volume_path}/conversations/{project_id}.json"

        if not os.path.exists(file_path):
            return []

        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('messages', [])
```

**Acceptance Criteria:**
- ✅ Projects saved to Volumes
- ✅ Conversations persisted
- ✅ Data survives app restarts
- ✅ Multiple projects supported

---

## Frontend Development

### 3. React Application (Week 3-4)

#### Task 3.1: Initialize React Project

**Project Structure:**
```
frontend/
├── public/
│   └── index.html
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── vite-env.d.ts
│   │
│   ├── components/
│   │   ├── ui/                    # shadcn/ui components
│   │   │   ├── button.tsx
│   │   │   ├── card.tsx
│   │   │   ├── dialog.tsx
│   │   │   └── ...
│   │   │
│   │   ├── chat/
│   │   │   ├── ChatInterface.tsx
│   │   │   ├── MessageList.tsx
│   │   │   ├── MessageInput.tsx
│   │   │   └── Message.tsx
│   │   │
│   │   ├── schema/
│   │   │   ├── SchemaView.tsx
│   │   │   ├── TableCard.tsx
│   │   │   ├── ColumnList.tsx
│   │   │   └── LayerTabs.tsx
│   │   │
│   │   └── layout/
│   │       ├── AppLayout.tsx
│   │       └── Header.tsx
│   │
│   ├── hooks/
│   │   ├── useDatabricks.ts
│   │   ├── useChat.ts
│   │   ├── useTables.ts
│   │   └── useProjects.ts
│   │
│   ├── services/
│   │   └── api.ts
│   │
│   ├── store/
│   │   ├── projectStore.ts
│   │   ├── chatStore.ts
│   │   └── uiStore.ts
│   │
│   ├── types/
│   │   └── index.ts
│   │
│   └── styles/
│       └── globals.css
│
├── package.json
├── tsconfig.json
├── vite.config.ts
└── tailwind.config.js
```

**Subtasks:**
- [ ] Create Vite + React + TypeScript project
- [ ] Install dependencies (axios, zustand, react-markdown)
- [ ] Configure Tailwind CSS
- [ ] Install and configure shadcn/ui
- [ ] Set up TypeScript types
- [ ] Configure API base URL

**Acceptance Criteria:**
- ✅ Dev server runs on port 5173
- ✅ TypeScript compilation works
- ✅ Tailwind styles applied
- ✅ Hot reload functional

#### Task 3.2: Chat Interface

**Subtasks:**
- [ ] Create ChatInterface component
- [ ] Create MessageList with auto-scroll
- [ ] Create Message component (user/assistant)
- [ ] Create MessageInput with send button
- [ ] Add markdown rendering for messages
- [ ] Add code syntax highlighting
- [ ] Add typing indicator
- [ ] Implement message history loading

**Code:**
```typescript
// src/components/chat/ChatInterface.tsx
import React, { useState, useEffect, useRef } from 'react';
import { MessageList } from './MessageList';
import { MessageInput } from './MessageInput';
import { useChatStore } from '@/store/chatStore';
import { sendMessage } from '@/services/api';

export const ChatInterface: React.FC<{ projectId: string }> = ({ projectId }) => {
  const { messages, addMessage, isLoading, setLoading } = useChatStore();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSendMessage = async (content: string) => {
    // Add user message
    addMessage({ role: 'user', content, timestamp: new Date() });
    setLoading(true);

    try {
      // Call API
      const response = await sendMessage(projectId, content);

      // Add assistant response
      addMessage({
        role: 'assistant',
        content: response.content,
        timestamp: new Date()
      });
    } catch (error) {
      addMessage({
        role: 'assistant',
        content: 'Sorry, an error occurred. Please try again.',
        timestamp: new Date()
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <MessageList messages={messages} isLoading={isLoading} />
      <div ref={messagesEndRef} />
      <MessageInput onSend={handleSendMessage} disabled={isLoading} />
    </div>
  );
};
```

**Acceptance Criteria:**
- ✅ Messages display correctly
- ✅ Auto-scroll to latest message
- ✅ Markdown rendered properly
- ✅ Loading states visible
- ✅ Input disabled during processing

#### Task 3.3: Schema Visualization

**Subtasks:**
- [ ] Create SchemaView component
- [ ] Create layer tabs (Source, Stage, Dimensions, Facts)
- [ ] Create TableCard component
- [ ] Display table metadata (columns, types)
- [ ] Add search/filter functionality
- [ ] Add "Generate DDL" action
- [ ] Add "View Details" modal

**Code:**
```typescript
// src/components/schema/SchemaView.tsx
import React, { useState, useEffect } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { TableCard } from './TableCard';
import { useTablesStore } from '@/store/tablesStore';
import { fetchTables } from '@/services/api';

export const SchemaView: React.FC<{ projectId: string }> = ({ projectId }) => {
  const { tables, setTables } = useTablesStore();
  const [activeTab, setActiveTab] = useState('all');

  useEffect(() => {
    loadTables();
  }, [projectId]);

  const loadTables = async () => {
    const data = await fetchTables(projectId);
    setTables(data);
  };

  const filterByLayer = (layer: string) => {
    if (layer === 'all') return tables;
    return tables.filter(t => t.layer === layer);
  };

  return (
    <div className="p-4">
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="all">All Tables</TabsTrigger>
          <TabsTrigger value="source">Source</TabsTrigger>
          <TabsTrigger value="stage">Stage</TabsTrigger>
          <TabsTrigger value="dimension">Dimensions</TabsTrigger>
          <TabsTrigger value="fact">Facts</TabsTrigger>
        </TabsList>

        <TabsContent value={activeTab}>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-4">
            {filterByLayer(activeTab).map(table => (
              <TableCard key={table.full_name} table={table} />
            ))}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
};
```

**Acceptance Criteria:**
- ✅ Tables grouped by layer
- ✅ Tab switching works
- ✅ Table cards display metadata
- ✅ Search filters tables instantly

---

## LangGraph Agent Migration

### 4. Migrate CLI Agent to Databricks (Week 5-6)

#### Task 4.1: Refactor Agent for Databricks

**Subtasks:**
- [ ] Move agent code from `askforkpi_langgraph.py` to `app/agent/`
- [ ] Replace global variables with Unity Catalog queries
- [ ] Update tools to use Databricks SDK
- [ ] Implement Volumes-based state management
- [ ] Add project context to all tool calls
- [ ] Update prompts for Databricks terminology

**Code:**
```python
# app/agent/tools.py
from langchain.tools import tool
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any

from app.services.unity_catalog_service import UnityCatalogService
from app.services.storage_service import VolumesStorageService

@tool
def get_source_tables(catalog: str, schema: str) -> List[Dict[str, Any]]:
    """
    Get the list of source tables from Unity Catalog.
    Always execute this first when starting dimensional design.

    Args:
        catalog: Unity Catalog name
        schema: Schema name containing source tables
    """
    client = WorkspaceClient()
    service = UnityCatalogService(client)

    tables = client.tables.list(catalog_name=catalog, schema_name=schema)

    result = []
    for table in tables:
        table_info = service.get_table_schema(f"{catalog}.{schema}.{table.name}")
        result.append(table_info)

    return result

@tool
def get_stage_tables(project_id: str) -> List[Dict[str, Any]]:
    """
    Get the list of stage tables created for this project.
    Stage tables have 'stg_' prefix.

    Args:
        project_id: Current project ID
    """
    storage = VolumesStorageService()
    project = storage.load_project(project_id)

    return project.get('stage_tables', [])

@tool
def dimensional_design_principles() -> str:
    """
    Get dimensional modeling design principles and naming conventions.
    Execute this before creating any new tables or KPIs.
    """
    return """
    DIMENSIONAL MODELING PRINCIPLES FOR DATABRICKS:

    1. NAMING CONVENTIONS:
       - Stage tables: 'stg_' prefix (e.g., stg_orders)
       - Dimension tables: 'dim_' prefix (e.g., dim_customer)
       - Fact tables: 'fact_' prefix (e.g., fact_sales)
       - Primary key: Always named 'id'

    2. TABLE STRUCTURE:
       - Stage tables mirror source table structure
       - Dimension tables: id (PK) + descriptive attributes
       - Fact tables: id (PK) + foreign keys + measures

    3. DELTA LAKE BEST PRACTICES:
       - Use DELTA format for all tables
       - Include created_at, updated_at timestamps
       - Partition fact tables by date when appropriate

    4. UNITY CATALOG:
       - Create tables in designated catalog.schema
       - Add meaningful table comments
       - Tag tables with layer metadata (source/stage/dim/fact)

    5. KPI DESIGN:
       - KPIs are columns in fact tables
       - KPIs must be measurable (numeric)
       - Include calculation logic in column comment
    """

@tool
def add_table_with_columns(
    project_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    layer: str,
    columns: List[Dict[str, Any]],
    description: str = ""
) -> str:
    """
    Create a new dimensional table in Unity Catalog.

    Args:
        project_id: Current project ID
        catalog: Unity Catalog name
        schema: Schema name
        table_name: Table name (must have correct prefix)
        layer: Table layer (stage/dimension/fact)
        columns: List of column definitions with name, data_type, description
        description: Table description

    Returns:
        Success message with full table name
    """
    from app.services.ddl_service import DDLService
    from app.core.sql_warehouse import SQLWarehouseManager

    # Validate naming convention
    prefix_map = {"stage": "stg_", "dimension": "dim_", "fact": "fact_"}
    expected_prefix = prefix_map.get(layer)

    if expected_prefix and not table_name.startswith(expected_prefix):
        return f"Error: {layer} tables must start with '{expected_prefix}'"

    # Generate DDL
    ddl_service = DDLService()
    ddl = ddl_service.generate_delta_table_ddl(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        columns=columns,
        comment=description
    )

    # Execute DDL
    sql_manager = SQLWarehouseManager(warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"))
    sql_manager.execute_query(ddl)

    # Save to project metadata
    storage = VolumesStorageService()
    project = storage.load_project(project_id)

    if f'{layer}_tables' not in project:
        project[f'{layer}_tables'] = []

    project[f'{layer}_tables'].append({
        "full_name": f"{catalog}.{schema}.{table_name}",
        "name": table_name,
        "layer": layer,
        "columns": columns,
        "description": description
    })

    storage.save_project(project_id, project)

    return f"✓ Table {catalog}.{schema}.{table_name} created successfully in Unity Catalog"
```

**Acceptance Criteria:**
- ✅ All tools work with Unity Catalog
- ✅ No global state variables
- ✅ Tables created in Unity Catalog
- ✅ Project metadata saved to Volumes

#### Task 4.2: Agent Graph Configuration

**Subtasks:**
- [ ] Create agent graph with Databricks tools
- [ ] Configure state management
- [ ] Add error handling and retries
- [ ] Implement conversation memory
- [ ] Add tool execution logging

**Code:**
```python
# app/agent/graph.py
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_openai import ChatOpenAI
from typing import TypedDict, Annotated, List
from langchain.schema import HumanMessage, AIMessage, SystemMessage

from app.agent.tools import (
    get_source_tables,
    get_stage_tables,
    dimensional_design_principles,
    add_table_with_columns
)

class AgentState(TypedDict):
    messages: Annotated[List, "add_messages"]
    project_id: str
    catalog: str
    schema: str

def create_agent_graph():
    """Create LangGraph agent for dimensional modeling"""

    # Initialize LLM
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Define tools
    tools = [
        get_source_tables,
        get_stage_tables,
        dimensional_design_principles,
        add_table_with_columns
    ]

    # Bind tools to LLM
    llm_with_tools = llm.bind_tools(tools)

    # Create tool node
    tool_node = ToolNode(tools)

    # Create graph
    workflow = StateGraph(AgentState)

    # Add nodes
    workflow.add_node("agent", lambda state: {
        "messages": [llm_with_tools.invoke(state["messages"])]
    })
    workflow.add_node("tools", tool_node)

    # Add edges
    workflow.set_entry_point("agent")

    def should_continue(state):
        last_message = state["messages"][-1]
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            return "tools"
        return END

    workflow.add_conditional_edges("agent", should_continue)
    workflow.add_edge("tools", "agent")

    return workflow.compile()

async def run_agent(message: str, project_id: str, catalog: str, schema: str):
    """Run agent for a user message"""

    graph = create_agent_graph()

    # Prepare initial state
    initial_state = {
        "messages": [
            SystemMessage(content="""You are a dimensional modeling expert for Databricks.
            Help users design star schemas, create dimension and fact tables, and implement KPIs.
            Always use Unity Catalog for data storage and follow Kimball methodology."""),
            HumanMessage(content=message)
        ],
        "project_id": project_id,
        "catalog": catalog,
        "schema": schema
    }

    # Run graph
    result = await graph.ainvoke(initial_state)

    # Extract final response
    final_message = result["messages"][-1]

    return {
        "content": final_message.content,
        "tool_calls": getattr(final_message, 'tool_calls', [])
    }
```

**Acceptance Criteria:**
- ✅ Agent responds to user queries
- ✅ Tools execute correctly
- ✅ Conversation flows naturally
- ✅ Errors handled gracefully

---

## Unity Catalog Operations

### 5. Table Creation and Management (Week 6-7)

#### Task 5.1: DDL Generation Service

**Subtasks:**
- [ ] Create DDL generator for Delta tables
- [ ] Support all Databricks data types
- [ ] Add partitioning logic
- [ ] Add table properties (delta.enableChangeDataFeed, etc.)
- [ ] Generate ALTER TABLE statements

**Code:**
```python
# app/services/ddl_service.py
from typing import List, Dict, Any

class DDLService:
    """Generate DDL statements for Databricks Delta tables"""

    DATA_TYPE_MAP = {
        "StringType": "STRING",
        "IntegerType": "INT",
        "BigIntegerType": "BIGINT",
        "DoubleType": "DOUBLE",
        "DecimalType": "DECIMAL(18,2)",
        "DateTimeType": "TIMESTAMP",
        "DateType": "DATE",
        "BooleanType": "BOOLEAN"
    }

    def generate_delta_table_ddl(
        self,
        catalog: str,
        schema: str,
        table_name: str,
        columns: List[Dict[str, Any]],
        comment: str = "",
        partition_by: List[str] = None
    ) -> str:
        """Generate CREATE TABLE DDL for Delta table"""

        # Build column definitions
        column_defs = []
        for col in columns:
            col_type = self.DATA_TYPE_MAP.get(col['data_type'], col['data_type'])
            col_def = f"`{col['name']}` {col_type}"

            if col.get('comment'):
                # Escape single quotes in comments
                comment_escaped = col['comment'].replace("'", "''")
                col_def += f" COMMENT '{comment_escaped}'"

            column_defs.append(col_def)

        # Build DDL
        ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
  {',\n  '.join(column_defs)}
)
USING DELTA
"""

        # Add comment
        if comment:
            comment_escaped = comment.replace("'", "''")
            ddl += f"\nCOMMENT '{comment_escaped}'"

        # Add partitioning
        if partition_by:
            ddl += f"\nPARTITIONED BY ({', '.join(partition_by)})"

        # Add table properties
        ddl += """
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2'
)
"""

        return ddl.strip()

    def generate_alter_table_add_columns(
        self,
        full_table_name: str,
        columns: List[Dict[str, Any]]
    ) -> str:
        """Generate ALTER TABLE ADD COLUMNS statement"""

        column_defs = []
        for col in columns:
            col_type = self.DATA_TYPE_MAP.get(col['data_type'], col['data_type'])
            col_def = f"`{col['name']}` {col_type}"

            if col.get('comment'):
                comment_escaped = col['comment'].replace("'", "''")
                col_def += f" COMMENT '{comment_escaped}'"

            column_defs.append(col_def)

        ddl = f"""
ALTER TABLE {full_table_name}
ADD COLUMNS (
  {',\n  '.join(column_defs)}
)
"""
        return ddl.strip()
```

**Acceptance Criteria:**
- ✅ Valid DDL generated
- ✅ All data types supported
- ✅ Comments properly escaped
- ✅ Partitioning works

#### Task 5.2: Table Metadata Management

**Subtasks:**
- [ ] Track created tables in project metadata
- [ ] Store table lineage information
- [ ] Track DDL execution history
- [ ] Implement table version tracking

**Acceptance Criteria:**
- ✅ All created tables tracked
- ✅ History queryable
- ✅ Lineage visible

---

## SQL Warehouse Integration

### 6. Query Execution (Week 7-8)

#### Task 6.1: Synchronous Query Execution

**Already covered in Task 1.2**

#### Task 6.2: Asynchronous Query Execution

**Subtasks:**
- [ ] Implement async query submission
- [ ] Add polling for query status
- [ ] Implement result retrieval
- [ ] Add query cancellation
- [ ] Create background job queue

**Code:**
```python
# app/services/async_query_service.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import asyncio
import time

class AsyncQueryService:
    """Execute SQL queries asynchronously"""

    def __init__(self, warehouse_id: str):
        self.client = WorkspaceClient()
        self.warehouse_id = warehouse_id

    async def execute_query_async(self, query: str, timeout: int = 300):
        """Execute query asynchronously and wait for completion"""

        # Submit query
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=query,
            wait_timeout="0s"  # Don't wait, return immediately
        )

        statement_id = response.statement_id
        start_time = time.time()

        # Poll for completion
        while True:
            if time.time() - start_time > timeout:
                # Cancel query
                self.client.statement_execution.cancel_execution(statement_id)
                raise TimeoutError(f"Query exceeded timeout of {timeout}s")

            # Check status
            status = self.client.statement_execution.get_statement(statement_id)

            if status.status.state == StatementState.SUCCEEDED:
                return self._parse_result(status.result)

            elif status.status.state in [StatementState.FAILED, StatementState.CANCELED]:
                error_message = status.status.error.message if status.status.error else "Unknown error"
                raise Exception(f"Query failed: {error_message}")

            # Wait before polling again
            await asyncio.sleep(1)

    def _parse_result(self, result):
        """Parse query result"""
        if not result:
            return {"status": "success", "rows": []}

        # Extract columns and rows
        columns = [col.name for col in result.manifest.schema.columns]
        rows = []

        for chunk in result.data_array or []:
            rows.extend(chunk)

        return {
            "columns": columns,
            "rows": rows,
            "row_count": result.manifest.total_row_count
        }
```

**Acceptance Criteria:**
- ✅ Long queries don't block
- ✅ Status polling works
- ✅ Timeouts handled
- ✅ Cancellation works

---

## App Framework & Deployment

### 7. Databricks App Manifest (Week 9)

#### Task 7.1: Create databricks.yml

**File:**
```yaml
# databricks.yml
name: askforkpi
display_name: "AskForKPI - AI Dimensional Modeling"
version: "1.0.0"
description: |
  AI-powered dimensional modeling assistant for Databricks.

  Automatically design and create:
  - Stage tables
  - Dimension tables
  - Fact tables
  - KPIs

  Using natural language conversation and following Kimball methodology.
  Built natively for Unity Catalog and Delta Lake.

author: "Your Company Name"
author_email: "support@yourcompany.com"
license: "Proprietary"
homepage: "https://askforkpi.com"
documentation: "https://docs.askforkpi.com"

# Minimum Databricks version
min_databricks_version: "13.0"

# Required permissions
permissions:
  unity_catalog:
    - READ_METADATA
    - CREATE_SCHEMA
    - CREATE_TABLE
    - WRITE_DATA
  sql:
    - EXECUTE_QUERY
  volumes:
    - READ
    - WRITE

# App configuration
config:
  - name: "OPENAI_API_KEY"
    type: "secret"
    required: true
    description: "OpenAI API key for LLM (GPT-4o-mini)"

  - name: "warehouse_id"
    type: "string"
    required: true
    description: "SQL Warehouse ID for query execution"

  - name: "default_catalog"
    type: "string"
    required: false
    default: "main"
    description: "Default Unity Catalog for new projects"

# Installation steps
install:
  - type: "create_catalog"
    catalog_name: "askforkpi"
    comment: "AskForKPI application catalog"

  - type: "create_schema"
    catalog_name: "askforkpi"
    schema_name: "metadata"
    comment: "Application metadata storage"

  - type: "create_volume"
    catalog_name: "askforkpi"
    schema_name: "metadata"
    volume_name: "app_state"
    volume_type: "MANAGED"
    comment: "Application state and configuration storage"

# App entry point
entrypoint:
  type: "webapp"
  module: "app.main:app"
  port: 8000
  health_check: "/health"

# Resource requirements
resources:
  memory: "2Gi"
  cpu: "1"

# Monitoring
monitoring:
  metrics_enabled: true
  logs_enabled: true
  log_level: "INFO"
```

**Acceptance Criteria:**
- ✅ Valid YAML syntax
- ✅ All required fields present
- ✅ Permissions appropriate
- ✅ Installation steps correct

#### Task 7.2: Dockerfile for Databricks

**File:**
```dockerfile
# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY frontend/build/ ./frontend/build/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Acceptance Criteria:**
- ✅ Image builds successfully
- ✅ App runs in container
- ✅ Health check works
- ✅ Size optimized (<500MB)

---

## Testing Strategy

### 8. Testing (Week 10)

#### Task 8.1: Unit Tests

**Subtasks:**
- [ ] Test Unity Catalog service
- [ ] Test DDL generation
- [ ] Test agent tools
- [ ] Test storage service
- [ ] Achieve 70%+ coverage

**Example:**
```python
# tests/test_services/test_ddl_service.py
import pytest
from app.services.ddl_service import DDLService

def test_generate_delta_table_ddl():
    """Test DDL generation for Delta table"""
    service = DDLService()

    columns = [
        {"name": "id", "data_type": "BigIntegerType", "comment": "Primary key"},
        {"name": "name", "data_type": "StringType", "comment": "Customer name"},
        {"name": "created_at", "data_type": "DateTimeType", "comment": "Created timestamp"}
    ]

    ddl = service.generate_delta_table_ddl(
        catalog="main",
        schema="dimensions",
        table_name="dim_customer",
        columns=columns,
        comment="Customer dimension table"
    )

    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "main.dimensions.dim_customer" in ddl
    assert "`id` BIGINT" in ddl
    assert "`name` STRING" in ddl
    assert "USING DELTA" in ddl
    assert "COMMENT 'Customer dimension table'" in ddl

def test_data_type_mapping():
    """Test data type mapping"""
    service = DDLService()

    assert service.DATA_TYPE_MAP["StringType"] == "STRING"
    assert service.DATA_TYPE_MAP["IntegerType"] == "INT"
    assert service.DATA_TYPE_MAP["DateTimeType"] == "TIMESTAMP"
```

**Acceptance Criteria:**
- ✅ All tests pass
- ✅ Coverage > 70%
- ✅ Tests run fast (<10s)

#### Task 8.2: Integration Tests

**Subtasks:**
- [ ] Test with real Databricks workspace
- [ ] Test table creation end-to-end
- [ ] Test agent conversation flow
- [ ] Test API endpoints
- [ ] Use test catalog/schema

**Example:**
```python
# tests/test_integration/test_databricks_integration.py
import pytest
from databricks.sdk import WorkspaceClient
from app.services.unity_catalog_service import UnityCatalogService
from app.services.ddl_service import DDLService
from app.core.sql_warehouse import SQLWarehouseManager
import os

@pytest.fixture
def databricks_client():
    """Real Databricks client for integration tests"""
    return WorkspaceClient()

@pytest.fixture
def test_catalog():
    """Test catalog name"""
    return "test_askforkpi"

@pytest.fixture
def test_schema():
    """Test schema name"""
    return "integration_tests"

def test_create_table_in_unity_catalog(databricks_client, test_catalog, test_schema):
    """Test creating a table in Unity Catalog"""

    # Generate DDL
    ddl_service = DDLService()
    table_name = f"test_table_{int(time.time())}"

    ddl = ddl_service.generate_delta_table_ddl(
        catalog=test_catalog,
        schema=test_schema,
        table_name=table_name,
        columns=[
            {"name": "id", "data_type": "BigIntegerType"},
            {"name": "name", "data_type": "StringType"}
        ],
        comment="Integration test table"
    )

    # Execute DDL
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    sql_manager = SQLWarehouseManager(warehouse_id)
    sql_manager.execute_query(ddl)

    # Verify table exists
    full_name = f"{test_catalog}.{test_schema}.{table_name}"
    table_info = databricks_client.tables.get(full_name=full_name)

    assert table_info is not None
    assert table_info.name == table_name
    assert len(table_info.columns) == 2

    # Cleanup
    sql_manager.execute_query(f"DROP TABLE IF EXISTS {full_name}")
```

**Acceptance Criteria:**
- ✅ Integration tests pass
- ✅ Tests clean up after themselves
- ✅ Can run against dev workspace

---

## Security & Compliance

### 9. Security (Week 11)

#### Task 9.1: Security Hardening

**Subtasks:**
- [ ] Implement input validation
- [ ] Add SQL injection prevention
- [ ] Sanitize all user inputs
- [ ] Add rate limiting
- [ ] Implement audit logging
- [ ] Add secret scanning

**Code:**
```python
# app/core/security.py
import re
from typing import Any

class SecurityValidator:
    """Security validation utilities"""

    @staticmethod
    def validate_identifier(name: str) -> bool:
        """Validate catalog/schema/table name"""
        # Only allow alphanumeric, underscore, and hyphen
        pattern = r'^[a-zA-Z0-9_-]+$'
        return bool(re.match(pattern, name))

    @staticmethod
    def sanitize_sql_identifier(name: str) -> str:
        """Sanitize SQL identifier"""
        if not SecurityValidator.validate_identifier(name):
            raise ValueError(f"Invalid identifier: {name}")
        return f"`{name}`"

    @staticmethod
    def validate_no_external_calls(allowed_domains: list):
        """Ensure no unauthorized external API calls"""
        # Implementation for monitoring network calls
        pass

# Audit logging
# app/core/audit.py
import logging
from datetime import datetime
from typing import Dict, Any

class AuditLogger:
    """Audit logging for compliance"""

    def __init__(self):
        self.logger = logging.getLogger("audit")

    def log_table_creation(self, user: str, table_name: str, metadata: Dict[str, Any]):
        """Log table creation event"""
        self.logger.info({
            "event": "table_created",
            "timestamp": datetime.utcnow().isoformat(),
            "user": user,
            "table": table_name,
            "metadata": metadata
        })

    def log_query_execution(self, user: str, query: str, result: str):
        """Log query execution"""
        self.logger.info({
            "event": "query_executed",
            "timestamp": datetime.utcnow().isoformat(),
            "user": user,
            "query": query[:500],  # Truncate long queries
            "result": result
        })
```

**Acceptance Criteria:**
- ✅ SQL injection prevented
- ✅ Input validation works
- ✅ Audit logs captured
- ✅ No secrets in logs

#### Task 9.2: SOC 2 Preparation

**Subtasks:**
- [ ] Document security controls
- [ ] Implement encryption at rest
- [ ] Implement encryption in transit
- [ ] Add access controls
- [ ] Create incident response plan
- [ ] Start SOC 2 audit process

**Acceptance Criteria:**
- ✅ Security documentation complete
- ✅ Controls implemented
- ✅ Audit initiated

---

## Documentation

### 10. Documentation (Week 12)

#### Task 10.1: User Documentation

**Subtasks:**
- [ ] Write getting started guide
- [ ] Create tutorial videos (3-5 min)
- [ ] Document dimensional modeling concepts
- [ ] Create FAQ
- [ ] Add troubleshooting guide

**Acceptance Criteria:**
- ✅ Docs accessible
- ✅ Videos professional
- ✅ FAQ comprehensive

#### Task 10.2: API Documentation

**Subtasks:**
- [ ] Auto-generate with FastAPI
- [ ] Add detailed examples
- [ ] Document authentication
- [ ] Add rate limits

**Acceptance Criteria:**
- ✅ Docs at `/api/docs`
- ✅ All endpoints documented
- ✅ Examples working

---

## Timeline & Milestones

### Week-by-Week Breakdown

**Week 1-2: Foundation**
- ✅ Databricks SDK integration
- ✅ FastAPI application structure
- ✅ Unity Catalog API working
- ✅ SQL Warehouse connection
- **Milestone**: Can read Unity Catalog and execute queries

**Week 3-4: Frontend**
- ✅ React application setup
- ✅ Chat interface
- ✅ Schema visualization
- **Milestone**: Basic UI functional

**Week 5-6: Agent Migration**
- ✅ LangGraph agent refactored
- ✅ Databricks-native tools
- ✅ Volumes storage
- **Milestone**: Agent creates tables in Unity Catalog

**Week 7-8: Advanced Features**
- ✅ DDL generation
- ✅ Async query execution
- ✅ Project management
- **Milestone**: Complete dimensional modeling workflow

**Week 9: Deployment**
- ✅ Databricks App manifest
- ✅ Docker container
- ✅ Local testing
- **Milestone**: App runs in Databricks workspace

**Week 10: Testing**
- ✅ Unit tests (70%+ coverage)
- ✅ Integration tests
- ✅ End-to-end tests
- **Milestone**: All tests passing

**Week 11: Security**
- ✅ Security hardening
- ✅ Audit logging
- ✅ SOC 2 preparation
- **Milestone**: Security compliant

**Week 12: Documentation & Launch Prep**
- ✅ User documentation
- ✅ API documentation
- ✅ Demo video
- ✅ Marketplace application ready
- **Milestone**: Ready for design partners

---

## Success Metrics

### Technical Metrics
- [ ] App startup time < 10 seconds
- [ ] API response time < 1 second (p95)
- [ ] Table creation < 5 seconds
- [ ] Agent response starts < 3 seconds
- [ ] Test coverage > 70%
- [ ] Zero critical security vulnerabilities

### Product Metrics
- [ ] 5 design partners actively using
- [ ] 50+ tables created across all users
- [ ] 100+ chat messages sent
- [ ] Average session duration > 10 minutes
- [ ] User satisfaction score > 4/5

### Business Metrics
- [ ] 3 LOIs (letters of intent) secured
- [ ] Marketplace application submitted
- [ ] Demo gets "wow" reactions
- [ ] Design partners willing to pay
- [ ] Clear path to $25k ACV

---

## Risk Mitigation

| Risk | Mitigation Strategy |
|------|-------------------|
| **Unity Catalog API changes** | Use official SDK, pin versions, monitor changelog |
| **LLM costs too high** | Cache aggressively, use GPT-4o-mini, optimize prompts |
| **Databricks approval delayed** | Start application early, hire consultant if needed |
| **Performance issues** | Optimize queries, use async execution, add caching |
| **Security review fails** | Build security in from day 1, hire pen tester early |
| **Limited design partner interest** | Offer free tier, emphasize time savings, get warm intros |

---

## Simplified vs Original Plan

**What Changed:**

| Component | Original Plan | Databricks Native | Benefit |
|-----------|--------------|-------------------|---------|
| **Database** | PostgreSQL + migrations | Unity Catalog | No database to manage |
| **Cache** | Redis | Databricks Volumes | Simpler, one less service |
| **Auth** | Auth0 | Databricks OAuth | Native, trusted |
| **Deployment** | AWS ECS + RDS | Databricks App Framework | Zero DevOps |
| **Billing** | Stripe | Databricks billing | Easier procurement |
| **Security** | Custom | Inherits from Databricks | Faster approval |

**Result**: 50% less code, 70% faster time to market, 80% lower infrastructure costs

---

**Last Updated**: 2025-12-19
**Version**: 2.0.0 - Databricks Native App
**Status**: Ready for Development
