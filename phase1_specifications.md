# Phase 1: MVP Specifications
## Detailed Technical Specifications and Subtasks

**Duration**: 3 months (12 weeks)
**Goal**: Transform CLI prototype into production-ready web application
**Success Criteria**: 10 beta users, 3 paying customers, $500+ MRR

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Backend Development](#backend-development)
3. [Frontend Development](#frontend-development)
4. [Database & Persistence](#database--persistence)
5. [Authentication & Authorization](#authentication--authorization)
6. [Database Connectors](#database-connectors)
7. [SQL DDL Generation](#sql-ddl-generation)
8. [DevOps & Infrastructure](#devops--infrastructure)
9. [Testing Strategy](#testing-strategy)
10. [Documentation](#documentation)
11. [Timeline & Milestones](#timeline--milestones)

---

## Architecture Overview

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  React App   │  │  Chat UI     │  │  Schema View │          │
│  │  (Vite)      │  │  Component   │  │  Component   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTPS / WebSocket
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          API Layer                               │
│  ┌──────────────────────────────────────────────────┐           │
│  │           FastAPI Application                     │           │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │           │
│  │  │ Auth Routes│  │ Chat Routes│  │Project API │ │           │
│  │  └────────────┘  └────────────┘  └────────────┘ │           │
│  └──────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
            ▼                 ▼                 ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  LangGraph      │  │  PostgreSQL     │  │  Redis          │
│  Agent Service  │  │  Database       │  │  Cache/Session  │
│                 │  │                 │  │                 │
│  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │
│  │ Tools     │  │  │  │ Users     │  │  │  │ Sessions  │  │
│  │ State Mgmt│  │  │  │ Projects  │  │  │  │ Cache     │  │
│  │ Memory    │  │  │  │ Tables    │  │  │  │ Queue     │  │
│  └───────────┘  │  │  │ Messages  │  │  │  └───────────┘  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │
         │ Connect to
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              External Data Warehouses                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Snowflake   │  │  Databricks  │  │  PostgreSQL  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Decisions

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Frontend Framework | React 18 + TypeScript | Industry standard, large ecosystem, TypeScript for reliability |
| Build Tool | Vite | Fast dev server, optimized builds |
| UI Framework | Tailwind CSS + shadcn/ui | Rapid development, professional components |
| State Management | Zustand | Lightweight, simple, sufficient for MVP |
| Backend Framework | FastAPI | High performance, automatic API docs, async support |
| Task Queue | Celery + Redis | Reliable async task processing |
| Database | PostgreSQL 15 | Robust, JSON support, proven reliability |
| Cache/Session | Redis 7 | Fast, pub/sub for real-time features |
| LLM Integration | LangGraph + LangChain | Current codebase compatibility |
| Authentication | Auth0 | Quick setup, secure, social logins |
| Deployment | Docker + Docker Compose | Consistent environments |
| Hosting | AWS (ECS/RDS/ElastiCache) | Industry standard, scalable |

---

## Backend Development

### 1. Project Structure

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                    # FastAPI application entry
│   ├── config.py                  # Configuration management
│   ├── dependencies.py            # Dependency injection
│   │
│   ├── api/                       # API routes
│   │   ├── __init__.py
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── auth.py           # Authentication endpoints
│   │   │   ├── projects.py       # Project CRUD
│   │   │   ├── chat.py           # Chat/conversation endpoints
│   │   │   ├── tables.py         # Table management
│   │   │   ├── connections.py    # Database connections
│   │   │   └── users.py          # User management
│   │
│   ├── models/                    # SQLAlchemy models
│   │   ├── __init__.py
│   │   ├── user.py
│   │   ├── project.py
│   │   ├── table.py
│   │   ├── message.py
│   │   ├── connection.py
│   │   └── workspace.py
│   │
│   ├── schemas/                   # Pydantic schemas
│   │   ├── __init__.py
│   │   ├── user.py
│   │   ├── project.py
│   │   ├── table.py
│   │   ├── message.py
│   │   └── connection.py
│   │
│   ├── services/                  # Business logic
│   │   ├── __init__.py
│   │   ├── agent_service.py      # LangGraph agent wrapper
│   │   ├── table_service.py      # Table operations
│   │   ├── ddl_service.py        # SQL DDL generation
│   │   ├── connection_service.py # Database connections
│   │   └── auth_service.py       # Authentication logic
│   │
│   ├── core/                      # Core functionality
│   │   ├── __init__.py
│   │   ├── security.py           # JWT, password hashing
│   │   ├── database.py           # Database session
│   │   ├── redis_client.py       # Redis connection
│   │   └── exceptions.py         # Custom exceptions
│   │
│   ├── agent/                     # LangGraph agent code
│   │   ├── __init__.py
│   │   ├── graph.py              # Agent graph definition
│   │   ├── tools.py              # Tool definitions
│   │   ├── state.py              # State management
│   │   └── prompts.py            # Prompt templates
│   │
│   ├── connectors/                # Database connectors
│   │   ├── __init__.py
│   │   ├── base.py               # Base connector interface
│   │   ├── snowflake.py
│   │   ├── databricks.py
│   │   └── postgres.py
│   │
│   └── utils/                     # Utilities
│       ├── __init__.py
│       ├── logger.py
│       ├── validators.py
│       └── helpers.py
│
├── alembic/                       # Database migrations
│   ├── versions/
│   └── env.py
│
├── tests/                         # Test suite
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_api/
│   ├── test_services/
│   └── test_agent/
│
├── requirements.txt
├── requirements-dev.txt
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

### 2. Backend Subtasks

#### 2.1 Core Setup (Week 1)

**Task 2.1.1: Initialize FastAPI Application**
- [ ] Create project structure as defined above
- [ ] Set up FastAPI app with CORS middleware
- [ ] Configure Pydantic settings management
- [ ] Add health check endpoint (`/health`, `/ready`)
- [ ] Set up logging (structured JSON logs)
- [ ] Configure environment variables (.env support)

**Acceptance Criteria:**
- Server starts on port 8000
- `/health` returns 200 OK
- Logs are JSON formatted
- Environment variables loaded correctly

**Task 2.1.2: Database Setup**
- [ ] Install SQLAlchemy 2.0 + asyncpg
- [ ] Create database connection pool
- [ ] Set up Alembic for migrations
- [ ] Create base model class with common fields (id, created_at, updated_at)
- [ ] Add database session dependency injection

**Acceptance Criteria:**
- Database connection pool works
- Migrations run successfully
- Session management handles rollbacks

**Task 2.1.3: Redis Setup**
- [ ] Install redis-py + aioredis
- [ ] Create Redis client wrapper
- [ ] Implement connection pooling
- [ ] Add health check for Redis
- [ ] Create helper methods (get, set, delete, expire)

**Acceptance Criteria:**
- Redis connection established
- Cache operations work
- Connection pool handles failures gracefully

#### 2.2 Authentication System (Week 2)

**Task 2.2.1: Auth0 Integration**
- [ ] Set up Auth0 tenant
- [ ] Configure social logins (Google, GitHub)
- [ ] Install python-jose for JWT
- [ ] Create JWT validation middleware
- [ ] Add dependency for `get_current_user`

**Acceptance Criteria:**
- JWT tokens validated correctly
- Unauthorized requests return 401
- User info extracted from token

**Task 2.2.2: User Management**
- [ ] Create User model (id, email, name, auth0_id, subscription_tier)
- [ ] Create User schema (Pydantic)
- [ ] Implement user CRUD operations
- [ ] Add user registration endpoint
- [ ] Add user profile endpoints (GET, PATCH)

**Acceptance Criteria:**
- Users can register
- Users can view/update profile
- Email uniqueness enforced

**Task 2.2.3: Authorization & Permissions**
- [ ] Define permission model (free, starter, professional)
- [ ] Create permission decorator
- [ ] Implement rate limiting (based on tier)
- [ ] Add project ownership validation

**Acceptance Criteria:**
- Free users limited to 3 projects
- Unauthorized access blocked
- Rate limiting works per tier

#### 2.3 Project Management (Week 3)

**Task 2.3.1: Project Model & Schema**
- [ ] Create Project model (id, name, description, user_id, workspace_id, settings)
- [ ] Create Project schema
- [ ] Add project CRUD operations
- [ ] Implement soft delete

**Acceptance Criteria:**
- Projects can be created, read, updated, deleted
- Soft delete preserves data
- User can only access own projects

**Task 2.3.2: Project API Endpoints**
- [ ] POST `/api/v1/projects` - Create project
- [ ] GET `/api/v1/projects` - List user's projects
- [ ] GET `/api/v1/projects/{id}` - Get project details
- [ ] PATCH `/api/v1/projects/{id}` - Update project
- [ ] DELETE `/api/v1/projects/{id}` - Delete project

**Acceptance Criteria:**
- All CRUD operations work
- Pagination implemented for list
- Proper error handling

**Task 2.3.3: Workspace/Tenant Isolation**
- [ ] Create Workspace model
- [ ] Add workspace_id to all relevant models
- [ ] Implement row-level security helpers
- [ ] Add workspace switching logic

**Acceptance Criteria:**
- Users can't access other workspaces
- Workspace isolation tested
- Multi-tenancy working

#### 2.4 LangGraph Agent Service (Week 4)

**Task 2.4.1: Migrate Current Agent Code**
- [ ] Move `askforkpi_langgraph.py` to `app/agent/`
- [ ] Refactor global variables to class-based state
- [ ] Update tools to use database instead of global lists
- [ ] Create AgentService wrapper class

**Acceptance Criteria:**
- Agent code runs in new structure
- No global state
- Tools read/write to database

**Task 2.4.2: Database-Backed Tool Implementation**
- [ ] Update `get_source_tables_with_columns` to read from DB
- [ ] Update `add_table_with_columns` to persist to DB
- [ ] Add project context to all tool calls
- [ ] Implement tool error handling

**Acceptance Criteria:**
- All tools work with database
- Tables persist across sessions
- Error messages logged properly

**Task 2.4.3: Conversation State Management**
- [ ] Create Message model (id, project_id, role, content, tool_calls)
- [ ] Implement conversation history loading
- [ ] Add checkpointer that uses PostgreSQL
- [ ] Implement conversation branching/forking

**Acceptance Criteria:**
- Conversations persist to database
- History loads correctly
- Multiple conversations per project

**Task 2.4.4: Async Agent Execution**
- [ ] Set up Celery with Redis broker
- [ ] Create async task for agent execution
- [ ] Implement task status tracking
- [ ] Add WebSocket for real-time updates

**Acceptance Criteria:**
- Long-running agent calls don't block API
- WebSocket streams responses
- Task status queryable

#### 2.5 Table Management (Week 5)

**Task 2.5.1: Table Models**
- [ ] Create Table model (id, project_id, name, layer, description, schema_json)
- [ ] Create Column model (id, table_id, name, data_type, description, sample_values)
- [ ] Add relationships between models
- [ ] Create schemas for API responses

**Acceptance Criteria:**
- Tables stored with full metadata
- Relationships work correctly
- JSON schema validated

**Task 2.5.2: Table API Endpoints**
- [ ] GET `/api/v1/projects/{id}/tables` - List tables
- [ ] GET `/api/v1/projects/{id}/tables/{table_id}` - Get table details
- [ ] POST `/api/v1/projects/{id}/tables` - Create table manually
- [ ] PATCH `/api/v1/projects/{id}/tables/{table_id}` - Update table
- [ ] DELETE `/api/v1/projects/{id}/tables/{table_id}` - Delete table

**Acceptance Criteria:**
- All table operations work
- Tables grouped by layer
- Validation prevents invalid schemas

**Task 2.5.3: Schema Validation**
- [ ] Validate table naming conventions (stg_, dim_, fact_)
- [ ] Validate column data types
- [ ] Check for duplicate column names
- [ ] Validate primary key requirements

**Acceptance Criteria:**
- Invalid schemas rejected
- Clear error messages
- Naming conventions enforced

#### 2.6 Database Connectors (Week 6)

**Task 2.6.1: Base Connector Interface**
- [ ] Define abstract base class for connectors
- [ ] Standard methods: connect, test_connection, get_schema, execute_ddl
- [ ] Error handling and retry logic
- [ ] Connection pooling

**Acceptance Criteria:**
- Interface defined clearly
- Common patterns abstracted
- Error handling consistent

**Task 2.6.2: PostgreSQL Connector**
- [ ] Implement PostgreSQL connector with psycopg2
- [ ] Add schema introspection
- [ ] Implement DDL execution
- [ ] Add transaction support

**Acceptance Criteria:**
- Can connect to Postgres
- Schema introspection works
- DDL executes successfully

**Task 2.6.3: Snowflake Connector**
- [ ] Install snowflake-connector-python
- [ ] Implement Snowflake connector
- [ ] Handle authentication (password, keypair, OAuth)
- [ ] Add schema introspection
- [ ] Implement DDL execution

**Acceptance Criteria:**
- Can connect to Snowflake
- All auth methods work
- DDL executes in Snowflake

**Task 2.6.4: Databricks Connector**
- [ ] Install databricks-sql-connector
- [ ] Implement Databricks connector
- [ ] Handle PAT token authentication
- [ ] Add Unity Catalog support
- [ ] Implement DDL execution

**Acceptance Criteria:**
- Can connect to Databricks
- Unity Catalog schemas readable
- DDL executes correctly

**Task 2.6.5: Connection Management API**
- [ ] Create Connection model (id, project_id, name, type, credentials_encrypted)
- [ ] Add credential encryption (Fernet)
- [ ] POST `/api/v1/connections` - Create connection
- [ ] GET `/api/v1/connections` - List connections
- [ ] POST `/api/v1/connections/{id}/test` - Test connection
- [ ] DELETE `/api/v1/connections/{id}` - Delete connection

**Acceptance Criteria:**
- Credentials stored encrypted
- Connection testing works
- Multiple connections per project

#### 2.7 SQL DDL Generation (Week 7)

**Task 2.7.1: DDL Generator Service**
- [ ] Create base DDL generator class
- [ ] Implement PostgreSQL DDL generator
- [ ] Implement Snowflake DDL generator
- [ ] Implement Databricks DDL generator
- [ ] Handle data type mapping

**Acceptance Criteria:**
- DDL generated correctly per dialect
- Data types mapped properly
- Valid SQL produced

**Task 2.7.2: DDL Preview & Execution**
- [ ] POST `/api/v1/projects/{id}/tables/{table_id}/generate-ddl` - Generate DDL
- [ ] POST `/api/v1/projects/{id}/tables/{table_id}/execute-ddl` - Execute DDL
- [ ] Add dry-run mode
- [ ] Log execution results

**Acceptance Criteria:**
- DDL preview shown before execution
- Execution creates actual tables
- Errors logged and returned

**Task 2.7.3: Batch Operations**
- [ ] POST `/api/v1/projects/{id}/generate-all-ddl` - Generate all DDL
- [ ] POST `/api/v1/projects/{id}/execute-all-ddl` - Execute all DDL
- [ ] Add transaction support (rollback on error)
- [ ] Show progress for batch operations

**Acceptance Criteria:**
- Can generate DDL for all tables
- Can execute in proper order (stage → dim → fact)
- Rollback works on error

#### 2.8 Chat API (Week 8)

**Task 2.8.1: Chat Endpoints**
- [ ] POST `/api/v1/projects/{id}/chat` - Send message
- [ ] GET `/api/v1/projects/{id}/chat/history` - Get conversation history
- [ ] DELETE `/api/v1/projects/{id}/chat` - Clear conversation
- [ ] WebSocket `/ws/projects/{id}/chat` - Real-time chat

**Acceptance Criteria:**
- Chat interface works
- History persists
- WebSocket streams responses

**Task 2.8.2: Streaming Responses**
- [ ] Implement SSE (Server-Sent Events) fallback
- [ ] Stream agent responses token-by-token
- [ ] Handle tool calls in stream
- [ ] Add typing indicators

**Acceptance Criteria:**
- Responses stream in real-time
- UI updates as agent thinks
- Tool calls visible to user

---

## Frontend Development

### 3. Project Structure

```
frontend/
├── public/
│   ├── favicon.ico
│   └── assets/
│
├── src/
│   ├── main.tsx                   # Entry point
│   ├── App.tsx                    # Root component
│   ├── vite-env.d.ts
│   │
│   ├── components/                # Reusable components
│   │   ├── ui/                    # shadcn/ui components
│   │   │   ├── button.tsx
│   │   │   ├── input.tsx
│   │   │   ├── dialog.tsx
│   │   │   ├── card.tsx
│   │   │   └── ...
│   │   │
│   │   ├── layout/
│   │   │   ├── AppLayout.tsx      # Main layout wrapper
│   │   │   ├── Sidebar.tsx        # Navigation sidebar
│   │   │   ├── Header.tsx         # Top header
│   │   │   └── Footer.tsx
│   │   │
│   │   ├── chat/
│   │   │   ├── ChatInterface.tsx  # Main chat component
│   │   │   ├── MessageList.tsx    # Message history
│   │   │   ├── MessageInput.tsx   # Input box
│   │   │   ├── Message.tsx        # Single message
│   │   │   └── TypingIndicator.tsx
│   │   │
│   │   ├── schema/
│   │   │   ├── SchemaView.tsx     # Schema visualization
│   │   │   ├── TableCard.tsx      # Single table card
│   │   │   ├── ColumnList.tsx     # Column list
│   │   │   ├── ERDiagram.tsx      # Entity-relationship diagram
│   │   │   └── LayerTabs.tsx      # Stage/Dim/Fact tabs
│   │   │
│   │   ├── project/
│   │   │   ├── ProjectList.tsx    # Project list
│   │   │   ├── ProjectCard.tsx    # Project card
│   │   │   ├── CreateProject.tsx  # Create project modal
│   │   │   └── ProjectSettings.tsx
│   │   │
│   │   └── connection/
│   │       ├── ConnectionList.tsx
│   │       ├── AddConnection.tsx
│   │       └── TestConnection.tsx
│   │
│   ├── pages/                     # Page components
│   │   ├── HomePage.tsx
│   │   ├── LoginPage.tsx
│   │   ├── DashboardPage.tsx
│   │   ├── ProjectPage.tsx
│   │   ├── SettingsPage.tsx
│   │   └── NotFoundPage.tsx
│   │
│   ├── hooks/                     # Custom React hooks
│   │   ├── useAuth.ts
│   │   ├── useProjects.ts
│   │   ├── useChat.ts
│   │   ├── useTables.ts
│   │   ├── useWebSocket.ts
│   │   └── useConnections.ts
│   │
│   ├── store/                     # State management (Zustand)
│   │   ├── authStore.ts
│   │   ├── projectStore.ts
│   │   ├── chatStore.ts
│   │   └── uiStore.ts
│   │
│   ├── services/                  # API service layer
│   │   ├── api.ts                 # Axios instance
│   │   ├── authService.ts
│   │   ├── projectService.ts
│   │   ├── chatService.ts
│   │   ├── tableService.ts
│   │   └── connectionService.ts
│   │
│   ├── types/                     # TypeScript types
│   │   ├── index.ts
│   │   ├── project.ts
│   │   ├── table.ts
│   │   ├── message.ts
│   │   └── user.ts
│   │
│   ├── utils/                     # Utility functions
│   │   ├── formatters.ts
│   │   ├── validators.ts
│   │   └── constants.ts
│   │
│   └── styles/                    # Global styles
│       ├── globals.css
│       └── tailwind.css
│
├── .env.example
├── .eslintrc.json
├── .prettierrc
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
└── tailwind.config.js
```

### 4. Frontend Subtasks

#### 4.1 Project Setup (Week 1)

**Task 4.1.1: Initialize React Project**
- [ ] Create Vite + React + TypeScript project
- [ ] Install dependencies (react-router-dom, zustand, axios)
- [ ] Configure Tailwind CSS
- [ ] Install shadcn/ui
- [ ] Set up ESLint + Prettier
- [ ] Configure path aliases (@/)

**Acceptance Criteria:**
- Dev server runs on port 5173
- Tailwind styling works
- TypeScript compilation successful

**Task 4.1.2: Routing Setup**
- [ ] Install react-router-dom v6
- [ ] Create route structure
- [ ] Set up protected routes
- [ ] Add 404 page
- [ ] Implement route guards

**Acceptance Criteria:**
- Navigation works
- Protected routes redirect to login
- URL structure clean

#### 4.2 Authentication UI (Week 2)

**Task 4.2.1: Auth0 Integration**
- [ ] Install @auth0/auth0-react
- [ ] Configure Auth0Provider
- [ ] Create useAuth hook wrapper
- [ ] Implement token refresh logic
- [ ] Add logout functionality

**Acceptance Criteria:**
- Users can log in via Auth0
- Tokens stored securely
- Auto-refresh works

**Task 4.2.2: Login/Signup Pages**
- [ ] Create LoginPage component
- [ ] Add social login buttons (Google, GitHub)
- [ ] Create loading states
- [ ] Add error handling
- [ ] Implement redirect after login

**Acceptance Criteria:**
- Login flow smooth
- Errors displayed clearly
- Redirects to dashboard after login

**Task 4.2.3: User Profile UI**
- [ ] Create profile dropdown in header
- [ ] Add profile settings page
- [ ] Implement avatar upload
- [ ] Add subscription tier display
- [ ] Create logout button

**Acceptance Criteria:**
- Profile accessible from header
- Settings page functional
- Logout works correctly

#### 4.3 Dashboard & Projects (Week 3)

**Task 4.3.1: Dashboard Layout**
- [ ] Create AppLayout component
- [ ] Build responsive sidebar
- [ ] Add header with user menu
- [ ] Implement mobile navigation
- [ ] Add breadcrumb navigation

**Acceptance Criteria:**
- Layout responsive on all screens
- Sidebar collapsible
- Navigation intuitive

**Task 4.3.2: Project List View**
- [ ] Create ProjectList component
- [ ] Add grid/list view toggle
- [ ] Implement search/filter
- [ ] Add sorting options
- [ ] Show project metadata (created date, table count)

**Acceptance Criteria:**
- Projects display correctly
- Search works instantly
- Sorting functional

**Task 4.3.3: Create Project Flow**
- [ ] Create modal/dialog for new project
- [ ] Add form validation
- [ ] Implement project templates (optional)
- [ ] Add loading states
- [ ] Success notification

**Acceptance Criteria:**
- Modal opens smoothly
- Validation prevents invalid submissions
- Project created successfully

#### 4.4 Chat Interface (Week 4-5)

**Task 4.4.1: Chat UI Layout**
- [ ] Create two-panel layout (chat + schema)
- [ ] Make panels resizable
- [ ] Add responsive mobile view (tabs)
- [ ] Implement scroll behavior

**Acceptance Criteria:**
- Layout clean and functional
- Panels resize smoothly
- Mobile view usable

**Task 4.4.2: Message Components**
- [ ] Create Message component (user/assistant)
- [ ] Add markdown rendering for assistant messages
- [ ] Implement code syntax highlighting
- [ ] Add copy button for code blocks
- [ ] Create typing indicator

**Acceptance Criteria:**
- Messages render correctly
- Markdown formatted properly
- Code blocks highlighted

**Task 4.4.3: Message Input**
- [ ] Create MessageInput component
- [ ] Add textarea with auto-resize
- [ ] Implement Enter to send, Shift+Enter for newline
- [ ] Add character count (if rate limited)
- [ ] Disable input during processing

**Acceptance Criteria:**
- Input feels natural
- Keyboard shortcuts work
- Loading states clear

**Task 4.4.4: Chat History**
- [ ] Implement infinite scroll for history
- [ ] Add date separators
- [ ] Create "Clear conversation" button
- [ ] Auto-scroll to bottom on new message
- [ ] Show message timestamps

**Acceptance Criteria:**
- History loads smoothly
- Auto-scroll works
- Clear conversation functional

**Task 4.4.5: Real-time Updates**
- [ ] Implement WebSocket connection
- [ ] Stream assistant responses
- [ ] Show token-by-token updates
- [ ] Handle reconnection logic
- [ ] Display connection status

**Acceptance Criteria:**
- WebSocket connects automatically
- Streaming smooth
- Reconnects on disconnect

#### 4.5 Schema Visualization (Week 6)

**Task 4.5.1: Schema View Layout**
- [ ] Create tabs for layers (All, Source, Stage, Dimensions, Facts)
- [ ] Implement grid layout for table cards
- [ ] Add expand/collapse for table details
- [ ] Create search/filter for tables

**Acceptance Criteria:**
- Tabs switch smoothly
- Tables organized clearly
- Search instant

**Task 4.5.2: Table Card Component**
- [ ] Display table name, layer, description
- [ ] Show column count
- [ ] Add actions menu (edit, delete, generate DDL)
- [ ] Implement hover effects
- [ ] Add table icons by layer

**Acceptance Criteria:**
- Cards look professional
- Actions accessible
- Icons intuitive

**Task 4.5.3: Table Details View**
- [ ] Create expandable section for columns
- [ ] Show column name, type, description, sample values
- [ ] Add copy DDL button
- [ ] Display relationships/foreign keys
- [ ] Show creation timestamp

**Acceptance Criteria:**
- Details comprehensive
- DDL copy works
- Layout clean

**Task 4.5.4: ER Diagram (Optional for MVP)**
- [ ] Install ReactFlow or similar
- [ ] Create node components for tables
- [ ] Draw edges for relationships
- [ ] Add zoom/pan controls
- [ ] Implement auto-layout

**Acceptance Criteria:**
- Diagram renders correctly
- Relationships visible
- Interactive navigation works

#### 4.6 Connection Management (Week 7)

**Task 4.6.1: Connection List**
- [ ] Create ConnectionList component
- [ ] Display connection cards
- [ ] Show connection status (active/inactive)
- [ ] Add test connection button
- [ ] Implement delete confirmation

**Acceptance Criteria:**
- Connections display correctly
- Status indicators clear
- Test connection works

**Task 4.6.2: Add Connection Flow**
- [ ] Create multi-step form
- [ ] Step 1: Select database type
- [ ] Step 2: Enter credentials
- [ ] Step 3: Test connection
- [ ] Step 4: Name and save
- [ ] Add validation per database type

**Acceptance Criteria:**
- Form guides user through process
- Validation prevents errors
- Connection saved successfully

**Task 4.6.3: Connection Security**
- [ ] Never display full credentials
- [ ] Mask sensitive fields
- [ ] Add "regenerate" for API keys
- [ ] Implement connection encryption indicator

**Acceptance Criteria:**
- Credentials never exposed
- Security indicators visible
- User feels safe

#### 4.7 Settings & Profile (Week 8)

**Task 4.7.1: Settings Page**
- [ ] Create tabbed settings layout
- [ ] Add profile settings tab
- [ ] Add subscription/billing tab
- [ ] Add preferences tab (theme, etc.)
- [ ] Add API keys tab

**Acceptance Criteria:**
- Settings organized clearly
- Tabs switch smoothly
- Changes saved

**Task 4.7.2: Subscription Management**
- [ ] Display current plan
- [ ] Show usage metrics (projects, tables)
- [ ] Add upgrade prompts
- [ ] Link to Stripe billing portal
- [ ] Show plan limits

**Acceptance Criteria:**
- Plan info accurate
- Upgrade path clear
- Billing portal accessible

---

## Database & Persistence

### 5. Database Schema

#### 5.1 Database Models (Week 2-3)

**Task 5.1.1: Users Table**
```sql
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auth0_id VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    avatar_url TEXT,
    subscription_tier VARCHAR(50) DEFAULT 'free',
    subscription_status VARCHAR(50),
    subscription_expires_at TIMESTAMP,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_login_at TIMESTAMP
);
```

**Task 5.1.2: Workspaces Table**
```sql
CREATE TABLE workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    owner_id UUID REFERENCES users(id) ON DELETE CASCADE,
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Task 5.1.3: Projects Table**
```sql
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    settings JSONB DEFAULT '{}',
    created_by UUID REFERENCES users(id),
    deleted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Task 5.1.4: Tables Table**
```sql
CREATE TABLE tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    layer VARCHAR(50) NOT NULL CHECK (layer IN ('source', 'stage', 'dimension', 'fact')),
    description TEXT,
    schema_json JSONB NOT NULL,
    created_by_message_id UUID,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(project_id, name)
);
```

**Task 5.1.5: Columns Table**
```sql
CREATE TABLE columns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id UUID REFERENCES tables(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    description TEXT,
    sample_values JSONB DEFAULT '[]',
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_foreign_key BOOLEAN DEFAULT FALSE,
    references_table_id UUID REFERENCES tables(id),
    references_column_id UUID REFERENCES columns(id),
    position INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(table_id, name)
);
```

**Task 5.1.6: Messages Table**
```sql
CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT,
    tool_calls JSONB,
    tool_call_id VARCHAR(255),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_messages_project_conversation ON messages(project_id, conversation_id, created_at);
```

**Task 5.1.7: Connections Table**
```sql
CREATE TABLE connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL CHECK (type IN ('postgres', 'snowflake', 'databricks')),
    credentials BYTEA NOT NULL, -- Encrypted
    metadata JSONB DEFAULT '{}',
    last_tested_at TIMESTAMP,
    test_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Task 5.1.8: Audit Logs Table (for Enterprise)**
```sql
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces(id),
    user_id UUID REFERENCES users(id),
    action VARCHAR(255) NOT NULL,
    resource_type VARCHAR(100),
    resource_id UUID,
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_workspace_created ON audit_logs(workspace_id, created_at DESC);
```

#### 5.2 Migrations (Week 3)

**Task 5.2.1: Alembic Setup**
- [ ] Initialize Alembic
- [ ] Create initial migration with all tables
- [ ] Add indexes for performance
- [ ] Create down migration
- [ ] Test migrations (up and down)

**Acceptance Criteria:**
- Migrations run successfully
- All tables created
- Indexes functional

**Task 5.2.2: Seed Data**
- [ ] Create seed script for development
- [ ] Add sample users
- [ ] Add sample projects
- [ ] Add sample tables
- [ ] Document seeding process

**Acceptance Criteria:**
- Seed data helps development
- Easy to reset database
- Documented in README

---

## Testing Strategy

### 6. Testing Subtasks (Ongoing, Week 1-12)

#### 6.1 Backend Testing

**Task 6.1.1: Unit Tests**
- [ ] Test database models
- [ ] Test services (agent, table, DDL, connection)
- [ ] Test utilities and helpers
- [ ] Test authentication logic
- [ ] Achieve 70%+ code coverage

**Acceptance Criteria:**
- All critical paths tested
- Tests run fast (<10s)
- Coverage report generated

**Task 6.1.2: Integration Tests**
- [ ] Test API endpoints
- [ ] Test database transactions
- [ ] Test agent + database integration
- [ ] Test WebSocket connections
- [ ] Use test database

**Acceptance Criteria:**
- All endpoints tested
- Database isolated per test
- Cleanup after tests

**Task 6.1.3: End-to-End Tests (Playwright/Cypress)**
- [ ] Test complete user flows
- [ ] Test chat conversation
- [ ] Test table creation workflow
- [ ] Test connection management
- [ ] Test DDL generation and execution

**Acceptance Criteria:**
- Critical user journeys covered
- Tests stable (not flaky)
- Run in CI/CD

#### 6.2 Frontend Testing

**Task 6.2.1: Component Tests (Vitest + Testing Library)**
- [ ] Test UI components
- [ ] Test forms and validation
- [ ] Test state management hooks
- [ ] Test routing
- [ ] Mock API calls

**Acceptance Criteria:**
- Components render correctly
- User interactions work
- Mocks prevent real API calls

**Task 6.2.2: Visual Regression Tests (Optional)**
- [ ] Set up Chromatic or Percy
- [ ] Snapshot critical pages
- [ ] Review visual diffs in PRs
- [ ] Document baseline images

**Acceptance Criteria:**
- Visual changes detected
- Easy to review diffs
- Integrated into CI

---

## DevOps & Infrastructure

### 7. Infrastructure Subtasks (Week 9-10)

#### 7.1 Docker Setup

**Task 7.1.1: Dockerfiles**
- [ ] Create Dockerfile for backend (multi-stage)
- [ ] Create Dockerfile for frontend
- [ ] Create Dockerfile for Celery worker
- [ ] Optimize image sizes
- [ ] Add health checks to containers

**Acceptance Criteria:**
- Images build successfully
- Sizes reasonable (<500MB)
- Health checks work

**Task 7.1.2: Docker Compose**
- [ ] Create docker-compose.yml for local dev
- [ ] Add services: backend, frontend, postgres, redis, celery
- [ ] Add volume mounts for development
- [ ] Configure networking
- [ ] Add environment variables

**Acceptance Criteria:**
- `docker-compose up` starts all services
- Hot reload works for development
- Services can communicate

**Task 7.1.3: Docker Compose for Production**
- [ ] Create docker-compose.prod.yml
- [ ] Use production builds
- [ ] Add nginx as reverse proxy
- [ ] Configure SSL/TLS
- [ ] Add monitoring containers (optional)

**Acceptance Criteria:**
- Production setup mirrors deployment
- SSL works locally
- Performance acceptable

#### 7.2 AWS Deployment

**Task 7.2.1: Infrastructure as Code (Terraform or CDK)**
- [ ] Set up Terraform/CDK project
- [ ] Define VPC and networking
- [ ] Define RDS PostgreSQL instance
- [ ] Define ElastiCache Redis cluster
- [ ] Define ECS cluster for containers
- [ ] Define ALB (Application Load Balancer)
- [ ] Define S3 bucket for static assets
- [ ] Define CloudFront distribution

**Acceptance Criteria:**
- Infrastructure reproducible
- Resources tagged properly
- Costs optimized

**Task 7.2.2: CI/CD Pipeline (GitHub Actions)**
- [ ] Create workflow for backend tests
- [ ] Create workflow for frontend tests
- [ ] Create workflow for Docker build and push
- [ ] Create workflow for deployment to AWS
- [ ] Add staging environment
- [ ] Add production environment (manual approval)

**Acceptance Criteria:**
- Tests run on every PR
- Deployment automated
- Staging updated automatically
- Production requires approval

**Task 7.2.3: Monitoring & Logging**
- [ ] Set up CloudWatch logs
- [ ] Create CloudWatch dashboards
- [ ] Add error tracking (Sentry)
- [ ] Set up uptime monitoring (UptimeRobot)
- [ ] Create alerts for critical metrics
- [ ] Add APM (Application Performance Monitoring)

**Acceptance Criteria:**
- Errors tracked and alerted
- Logs searchable
- Dashboards useful

**Task 7.2.4: Secrets Management**
- [ ] Use AWS Secrets Manager for credentials
- [ ] Rotate database passwords
- [ ] Store API keys securely
- [ ] Configure IAM roles properly
- [ ] Audit secret access

**Acceptance Criteria:**
- No secrets in code
- Rotation automated
- Access audited

#### 7.3 Domain & SSL

**Task 7.3.1: Domain Setup**
- [ ] Register domain (e.g., askforkpi.com)
- [ ] Configure DNS in Route 53
- [ ] Set up subdomains (app., api., www.)
- [ ] Configure SPF/DKIM for emails

**Acceptance Criteria:**
- Domain resolves correctly
- Subdomains work
- Email deliverability good

**Task 7.3.2: SSL Certificates**
- [ ] Request ACM certificate
- [ ] Validate certificate
- [ ] Configure ALB to use SSL
- [ ] Force HTTPS redirect
- [ ] Set up HSTS headers

**Acceptance Criteria:**
- HTTPS works
- Certificate auto-renews
- Security headers set

---

## Documentation

### 8. Documentation Subtasks (Week 11-12)

**Task 8.1: API Documentation**
- [ ] Set up FastAPI auto-generated docs
- [ ] Add detailed docstrings to endpoints
- [ ] Create Postman collection
- [ ] Add authentication guide
- [ ] Document rate limits

**Acceptance Criteria:**
- Docs accessible at /docs
- All endpoints documented
- Examples provided

**Task 8.2: User Documentation**
- [ ] Create getting started guide
- [ ] Add tutorial videos
- [ ] Document dimensional modeling concepts
- [ ] Create FAQ
- [ ] Add troubleshooting guide

**Acceptance Criteria:**
- New users can onboard quickly
- Common questions answered
- Videos professional

**Task 8.3: Developer Documentation**
- [ ] Update README.md
- [ ] Document local setup
- [ ] Add contributing guide
- [ ] Document architecture decisions
- [ ] Create runbook for operations

**Acceptance Criteria:**
- Developers can set up locally
- Architecture clear
- Operations documented

---

## Timeline & Milestones

### Week 1-2: Foundation
- ✓ Backend: FastAPI setup, database, Redis, authentication
- ✓ Frontend: React setup, routing, Auth0 integration
- **Milestone**: User can sign up and log in

### Week 3-4: Core Features
- ✓ Backend: Projects, LangGraph agent migration, conversation API
- ✓ Frontend: Dashboard, project management, basic chat UI
- **Milestone**: User can create project and have basic conversation

### Week 5-6: Schema Management
- ✓ Backend: Table models, table API, database connectors
- ✓ Frontend: Schema visualization, table cards, connection UI
- **Milestone**: User can see generated tables and manage connections

### Week 7-8: Advanced Features
- ✓ Backend: DDL generation, batch operations, WebSocket
- ✓ Frontend: Real-time chat, DDL preview/execution, settings
- **Milestone**: User can generate and execute DDL in real database

### Week 9-10: Infrastructure
- ✓ Docker setup
- ✓ AWS deployment
- ✓ CI/CD pipeline
- ✓ Monitoring and logging
- **Milestone**: Application deployed and accessible

### Week 11-12: Polish & Launch
- ✓ Comprehensive testing
- ✓ Documentation
- ✓ Performance optimization
- ✓ Security audit
- ✓ Beta user onboarding
- **Milestone**: Beta launch with first 10 users

---

## Success Metrics

### Technical Metrics
- [ ] API response time < 200ms (p95)
- [ ] Frontend load time < 2s
- [ ] Agent response starts streaming < 3s
- [ ] Test coverage > 70%
- [ ] Zero critical security vulnerabilities
- [ ] Uptime > 99.5%

### Product Metrics
- [ ] 10 beta users actively using product
- [ ] 50+ tables created across all users
- [ ] 200+ chat messages sent
- [ ] 10+ database connections configured
- [ ] 5+ successful DDL executions

### Business Metrics
- [ ] 3 paying customers ($147+ MRR)
- [ ] NPS score > 40
- [ ] Average session duration > 10 minutes
- [ ] User activation rate > 60% (create first table)
- [ ] Retention rate > 40% (week 2)

---

## Risk Mitigation

| Risk | Mitigation Strategy |
|------|-------------------|
| **LLM costs exceed budget** | Implement request caching, add rate limiting, use GPT-4o-mini |
| **Performance issues with large schemas** | Add pagination, lazy loading, database indexing |
| **Database connection security** | Encrypt credentials, use VPC, implement audit logs |
| **WebSocket stability** | Add reconnection logic, fallback to polling |
| **Scope creep** | Strict feature freeze after Week 10 |
| **Third-party API downtime** | Implement circuit breakers, fallback mechanisms |

---

## Post-MVP Roadmap (Phase 2 Preview)

**Week 13-16:**
- dbt integration (generate dbt models from tables)
- Data lineage visualization
- Team collaboration (sharing, comments)
- Version control for schemas

**Week 17-20:**
- Cost estimation for queries
- Query builder UI
- Automated testing for data models
- Git sync for schemas

---

**Last Updated**: 2025-12-19
**Version**: 1.0.0
**Status**: Ready for Development
