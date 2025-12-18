# CLAUDE.md - AI Assistant Guide for AskForKPI

## Project Overview

**AskForKPI** is a dimensional design project that enables users to provide their current database schema and ask for Key Performance Indicators (KPIs). The system uses AI agents to automatically design and create the necessary tables following dimensional modeling best practices (Kimball methodology).

### Key Capabilities
- Interactive AI-driven dimensional data modeling
- Automatic table generation (stage, dimension, and fact tables)
- KPI design and implementation guidance
- Schema exploration and documentation
- Conversational interface for data modeling tasks

### Technology Stack
- **AI Framework**: LangGraph for agent orchestration
- **LLM Integration**: LangChain with OpenAI (GPT-4o-mini) and Ollama support
- **Programming Language**: Python 3.x
- **Additional Tools**: LlamaIndex for advanced indexing capabilities

---

## Repository Structure

```
AskForKPI/
├── askforkpi_langgraph.py        # Main application: LangGraph agent implementation
├── custom_struct_type.py         # Custom type definitions for database schemas
├── requirements.txt              # Python dependencies
├── README.md                     # Project overview and documentation
├── img/                          # Sample output screenshots
│   ├── 2024-09-11_20.52.29.jpeg
│   └── 2024-09-11_20.52.36.jpeg
└── CLAUDE.md                     # This file: AI assistant development guide
```

### File Purposes

#### `askforkpi_langgraph.py` (Main Application - 216 lines)
The core agent implementation using LangGraph. Contains:
- Tool definitions for table operations
- State management with TypedDict
- LangGraph state machine configuration
- Assistant class for handling LLM interactions
- Sample tutorial questions for testing

**Key Components**:
- **State Graph**: Orchestrates conversation flow between assistant and tools
- **Tools**: 5 custom tools for dimensional modeling operations
- **Memory**: MemorySaver for conversation persistence
- **Assistant**: Wrapper class handling LLM invocations with error handling

#### `custom_struct_type.py` (Schema Definitions - 159 lines)
Custom type system for database schema representation. Contains:
- Data type classes (StringType, IntegerType, DateTimeType, etc.)
- StructField and StructType for schema composition
- TableType for complete table definitions
- SampleSchema with pre-defined source tables (Customers, Orders, Products, Inventory)

**Purpose**: Provides a structured way to define and manipulate database schemas without depending on Spark or other database-specific libraries.

---

## Architecture and Design Patterns

### Three-Layer Dimensional Architecture

The system follows a standard dimensional modeling architecture:

```
Source Layer → Stage Layer → Dimensional Layer (Dimensions + Facts)
```

1. **Source Layer**: Raw data from operational systems
   - Contains original tables (Customers, Orders, Products, Inventory)
   - No transformations applied

2. **Stage Layer**: Intermediate staging area
   - Prefix: `stg_`
   - Mirrors source table structure
   - Prepares data for dimensional modeling

3. **Dimensional Layer**: Analytics-optimized tables
   - **Dimensions** (prefix: `dim_`): Descriptive attributes
   - **Facts** (prefix: `fact_`): Measurable metrics and KPIs
   - Primary key convention: `id`

### LangGraph Agent Pattern

The application uses a **ReAct-style agent** with tool calling:

```
User Input → Assistant → Tool Selection → Tool Execution → Assistant → Response
                ↑                                                  ↓
                └──────────────── Loop until complete ────────────┘
```

**State Management**:
- `State` TypedDict with `messages` list (annotated with `add_messages`)
- `MemorySaver` checkpointer for conversation persistence
- Thread-based sessions with unique `thread_id`

**Error Handling**:
- Tool fallbacks with `handle_tool_error`
- Empty response retry logic in `Assistant.__call__`
- Error messages passed back to LLM for self-correction

---

## Key Components Deep Dive

### Tools (askforkpi_langgraph.py)

#### 1. `get_source_tables_with_columns()`
- **Purpose**: Retrieve source layer tables and columns
- **Returns**: List of JSON-formatted table definitions
- **Usage**: Always execute first when starting dimensional design
- **Example Output**: Customer, Orders, Products, Inventory schemas

#### 2. `get_stage_tables_with_columns()`
- **Purpose**: Retrieve stage layer tables
- **Returns**: List of created stage tables with columns
- **State**: Managed in `stage_tables` global list

#### 3. `get_dimension_and_facts_tables_with_columns()`
- **Purpose**: Retrieve dimension and fact tables
- **Returns**: List of dimensional layer tables
- **State**: Managed in `dimension_tables` global list

#### 4. `dimensional_design_principles()`
- **Purpose**: Return design guidelines and naming conventions
- **Critical**: Must be executed before creating new tables/KPIs
- **Returns**: String with all naming conventions and rules

#### 5. `add_table_with_columns(layer_name, table_name, columns, description)`
- **Purpose**: Create new table with columns in specified layer
- **Parameters**:
  - `layer_name`: "stage", "dimension", or "fact"
  - `table_name`: Table name with appropriate prefix
  - `columns`: List of column dictionaries with name, data_type, description, sample_values
  - `description`: Table description
- **Side Effect**: Appends to `stage_tables` list
- **Returns**: Success confirmation message

### Global State Variables

```python
source_tables = SampleSchema.get_source_tables_with_columns()  # Pre-loaded
stage_tables = []      # Dynamically populated
dimension_tables = []  # Dynamically populated
fact_tables = []       # Dynamically populated
```

**Important**: These are module-level globals that persist across agent invocations within the same process.

### Custom Type System

The type system in `custom_struct_type.py` provides:

**Data Types**:
- `StringType`, `IntegerType`, `DateTimeType`, `DoubleType`, `DecimalType`, `BooleanType`
- All types implement `__str__()` for serialization

**Schema Building**:
```python
schema = StructType()
schema.add_field("name", StringType(), description="...", sample_values=[...])
schema.add_struct("address", address_schema, description="...")
```

**Serialization**:
- `to_dict()` methods for JSON conversion
- `StructField` and `StructType` support nested structures

---

## Dimensional Design Conventions

### Naming Conventions (Enforced by Tools)

| Layer | Prefix | Example | Primary Key |
|-------|--------|---------|-------------|
| Source | (none) | `Customers`, `Orders` | Varies |
| Stage | `stg_` | `stg_orders` | Inherited from source |
| Dimension | `dim_` | `dim_customer`, `dim_date` | `id` |
| Fact | `fact_` | `fact_sales`, `fact_orders` | `id` |

### Design Principles

1. **Every source table should have a corresponding stage table**
2. **Primary key in dimensional tables is always `id`**
3. **Foreign keys in fact tables reference dimension PKs**
4. **KPIs are columns in fact tables**
5. **Dimension tables include PK + descriptive attributes**
6. **Fact tables include FKs + measurable metrics**

### Column Structure

Each column definition must include:
```python
{
    "name": "column_name",
    "data_type": "IntegerType",  # or StringType, DateTimeType, etc.
    "description": "Descriptive text",
    "sample_values": [1, 2, 3]   # Representative examples
}
```

---

## Development Workflows

### Adding New Source Tables

1. **Define schema in `custom_struct_type.py`**:
   ```python
   @staticmethod
   def get_new_table_schema():
       schema = StructType()
       schema.add_field("id", IntegerType(), description="...", sample_values=[...])
       # Add more fields...
       return json.dumps(schema.to_dict())
   ```

2. **Update `get_source_tables_with_columns()`**:
   Add JSON string to the returned list

3. **Test with agent**:
   Run queries asking about the new table

### Adding New Tools

1. **Define tool function with `@tool` decorator**:
   ```python
   @tool
   def new_tool_name(param: str) -> str:
       """Clear description for LLM to understand when to use this tool."""
       # Implementation
       return result
   ```

2. **Add to `datamodel_tools` list**:
   ```python
   datamodel_tools = [
       # existing tools...
       new_tool_name,
   ]
   ```

3. **Update tool node**:
   The `create_tool_node_with_fallback(datamodel_tools)` automatically includes new tools

### Modifying the Assistant Prompt

Edit `primary_assistant_prompt` in `askforkpi_langgraph.py:152`:
```python
primary_assistant_prompt = ChatPromptTemplate.from_messages([
    ("system", "Your custom system prompt here..."),
    ("placeholder", "{messages}"),
])
```

### Changing the LLM Model

Modify line 150:
```python
llm = ChatOpenAI(model="gpt-4o-mini")  # Change to gpt-4, gpt-3.5-turbo, etc.
```

For Ollama (local models):
```python
from langchain_ollama import ChatOllama
llm = ChatOllama(model="llama3")
```

---

## Testing and Debugging

### Running the Application

```bash
python askforkpi_langgraph.py
```

### Tutorial Questions

The script includes sample questions in `tutorial_questions` (lines 191-200):
```python
tutorial_questions = [
    "I would like a KPI for the total amount of orders by date and customer. Create the missing tables!",
]
```

**Modify these** to test different scenarios:
- Table creation workflows
- KPI design requests
- Schema exploration queries

### Debug Output

The `_print_event()` function (lines 102-117) controls output:
- Prints current dialog state
- Shows message content with truncation (max 1500 chars)
- Tracks printed messages to avoid duplicates

**Customize truncation**:
```python
max_length=1500  # Increase for more verbose output
```

### Memory and State

Each conversation uses a unique `thread_id`:
```python
thread_id = str(uuid.uuid4())
config = {"configurable": {"thread_id": thread_id}}
```

**For testing**: Use the same `thread_id` to maintain conversation context across runs.

---

## Common Development Tasks

### Task: Add Support for New Data Types

1. Define type class in `custom_struct_type.py`:
   ```python
   class TimestampType:
       def __str__(self):
           return "TimestampType"
   ```

2. Update documentation in tools that reference data types

3. Test with sample schema using new type

### Task: Implement Table Validation

Add validation logic in `add_table_with_columns()`:
```python
@tool
def add_table_with_columns(layer_name: str, table_name: str, columns: List[dict], description: str = "") -> str:
    # Validate layer name
    if layer_name not in ["stage", "dimension", "fact"]:
        return f"Error: Invalid layer '{layer_name}'. Must be stage, dimension, or fact."

    # Validate naming convention
    prefix_map = {"stage": "stg_", "dimension": "dim_", "fact": "fact_"}
    if not table_name.startswith(prefix_map[layer_name]):
        return f"Error: {layer_name} tables must start with '{prefix_map[layer_name]}'"

    # Existing implementation...
```

### Task: Add Visualization Support

The repository includes visualization capability (imported but not used):
```python
from IPython.display import Image, display
```

To visualize the graph:
```python
display(Image(part_1_graph.get_graph().draw_mermaid_png()))
```

### Task: Export Table Definitions

Add a new tool to export created tables:
```python
@tool
def export_tables_to_json(layer: str) -> str:
    """Export all tables from a specific layer to JSON format."""
    import json

    if layer == "stage":
        tables = stage_tables
    elif layer in ["dimension", "fact"]:
        tables = dimension_tables
    else:
        return "Invalid layer"

    return json.dumps(tables, indent=2)
```

---

## Code Quality and Best Practices

### Current Code Issues to Be Aware Of

1. **Duplicate imports** (lines 1-3 in askforkpi_langgraph.py):
   ```python
   from typing import List  # Appears twice
   from typing_extensions import TypedDict  # Also imported from typing
   ```

2. **Syntax error in line 63**:
   ```python
   stage_tables.append({"layer_name":layer_name, "table_name": table_name, description: description, "columns": columns})
   # Should be: "description": description
   ```

3. **Global state management**: Using module-level lists can cause issues in production. Consider:
   - Database persistence
   - Redis/cache storage
   - State within LangGraph checkpointer

4. **Unused imports**: `create_react_agent` imported but not used

5. **Magic strings**: String literals for layers ("stage", "dimension", "fact") should be constants

### Recommended Improvements

#### Use Constants
```python
# At top of askforkpi_langgraph.py
LAYER_SOURCE = "source"
LAYER_STAGE = "stage"
LAYER_DIMENSION = "dimension"
LAYER_FACT = "fact"

PREFIX_STAGE = "stg_"
PREFIX_DIMENSION = "dim_"
PREFIX_FACT = "fact_"
```

#### Add Type Hints
```python
from typing import List, Dict, Any

stage_tables: List[Dict[str, Any]] = []
```

#### Implement Proper State Management
Consider moving global lists into the State TypedDict:
```python
class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    stage_tables: List[Dict[str, Any]]
    dimension_tables: List[Dict[str, Any]]
    fact_tables: List[Dict[str, Any]]
```

---

## Environment Setup

### Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Required Environment Variables

```bash
# OpenAI API key (required for default configuration)
export OPENAI_API_KEY="sk-..."

# Optional: For Ollama usage
export OLLAMA_BASE_URL="http://localhost:11434"
```

### Dependencies Breakdown

- **langchain**: Core LangChain library for LLM orchestration
- **langgraph**: Graph-based agent workflow framework
- **openai**: OpenAI API client
- **langchain_openai**: LangChain integration for OpenAI models
- **langchain_ollama**: LangChain integration for Ollama (local models)
- **llama_index**: Advanced indexing and retrieval capabilities
- **llama-index-llms-ollama**: LlamaIndex Ollama integration
- **llama-index-utils-workflow**: Workflow utilities for LlamaIndex

---

## Future Development Roadmap

### Planned Features

1. **Databricks Integration** (mentioned in README.md)
   - Connect to Databricks workspaces
   - Execute actual SQL DDL statements
   - Sync with Unity Catalog

2. **Potential Enhancements**:
   - SQL query generation for KPIs
   - Automated ETL pipeline generation
   - Data lineage tracking
   - Integration with dbt (data build tool)
   - Support for additional modeling methodologies (Data Vault, etc.)
   - Web UI for non-technical users
   - Multi-tenancy support
   - Version control for table schemas
   - Automated testing of generated tables

### Architecture Evolution

Consider these architectural improvements:

1. **Persistence Layer**: Replace global lists with proper database
2. **API Layer**: REST or GraphQL API for programmatic access
3. **Authentication**: User management and access control
4. **Monitoring**: Logging, metrics, and observability
5. **CI/CD**: Automated testing and deployment pipelines

---

## AI Assistant Guidelines

### When Working on This Codebase

1. **Always check dimensional design principles** before creating tables
   - Use `dimensional_design_principles()` tool results
   - Verify naming conventions match (stg_, dim_, fact_)
   - Ensure primary key is `id` for dimensional tables

2. **Understand the workflow**:
   - Source tables are pre-loaded and immutable
   - Stage tables mirror source structure
   - Dimensions and facts are derived from stage tables
   - KPIs are fact table columns

3. **Maintain consistency**:
   - Follow existing code style
   - Use type hints where possible
   - Add docstrings to new tools
   - Update this CLAUDE.md when adding major features

4. **Testing approach**:
   - Modify `tutorial_questions` for new scenarios
   - Verify tool outputs manually
   - Check state persistence across conversation turns
   - Validate generated table structures

5. **Error handling**:
   - Tools should return descriptive error messages
   - Never raise exceptions in tools (return error strings instead)
   - Let the LLM self-correct using error feedback

### Common Pitfalls to Avoid

1. **Don't modify source tables** - they're pre-loaded and should remain constant
2. **Don't skip dimensional_design_principles** - it's critical for correct table design
3. **Don't forget sample_values** - they help the LLM understand data patterns
4. **Don't use database-specific syntax** - keep schemas generic
5. **Don't hardcode thread_id** - generate unique IDs for each session

### Code Review Checklist

- [ ] New tools have clear docstrings
- [ ] Naming conventions followed (stg_, dim_, fact_)
- [ ] Type hints added to functions
- [ ] Error handling implemented
- [ ] Global state updated appropriately
- [ ] Documentation updated (this file)
- [ ] Sample questions include new functionality
- [ ] No breaking changes to existing tools

---

## Quick Reference

### File Locations
- Main agent: `askforkpi_langgraph.py`
- Schema types: `custom_struct_type.py`
- Dependencies: `requirements.txt`
- This guide: `CLAUDE.md`

### Key Functions
- Tools: Lines 33-79 in `askforkpi_langgraph.py`
- State graph: Lines 176-189
- Assistant class: Lines 126-147
- Sample schemas: `SampleSchema` class in `custom_struct_type.py`

### LLM Configuration
- Model: Line 150 (`ChatOpenAI(model="gpt-4o-mini")`)
- Prompt: Lines 152-162 (`primary_assistant_prompt`)
- Tool binding: Line 172 (`llm.bind_tools(datamodel_tools)`)

### Running the Code
```bash
export OPENAI_API_KEY="your-key"
python askforkpi_langgraph.py
```

---

## Contact and Support

This is an open-source dimensional modeling project. For questions or contributions:
- Review this CLAUDE.md for architecture understanding
- Check README.md for project overview
- Examine sample outputs in `img/` directory
- Modify `tutorial_questions` for testing

**Last Updated**: 2025-12-18
**Version**: 1.0.0
