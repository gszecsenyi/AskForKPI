from typing import Annotated, List, TypedDict
from typing import List
from typing_extensions import TypedDict
from langgraph.graph.message import AnyMessage, add_messages
from langchain_core.runnables import Runnable, RunnableConfig
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.graph import END, StateGraph, START
import uuid
from langchain_openai.chat_models import ChatOpenAI
from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import tools_condition
from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableLambda

from langgraph.prebuilt import ToolNode
from IPython.display import Image, display

from langgraph.prebuilt import create_react_agent

import functools



from custom_struct_type import SampleSchema

source_tables = SampleSchema.get_source_tables_with_columns()
stage_tables = []
dimension_tables = []
fact_tables = []

@tool
def get_source_tables_with_columns() -> List[dict]:
    """Get the list of existing source layer table and his columns in JSON format. Execute always as first, and then the 'dimensional_design_principles' tool."""
    return source_tables

@tool
def get_stage_tables_with_columns() -> List[dict]:
    """Get the list of existing stage layer table and his columns in JSON format"""
    return stage_tables

@tool
def get_dimension_and_facts_tables_with_columns() -> List[dict]:
    """Get the list of existing dimension and facts table and his columns in JSON format"""
    return dimension_tables

@tool
def add_table_with_columns(layer_name: str, table_name: str, columns: List[dict], description: str = "") -> str:
    """ Create a new table with columns on a layer.
    Sample input:
        layer_name = "stage"
        table_name = "stg_orders"
        columns = [
            {"name": "order_id", "data_type": "IntegerType", "description": "Order ID", "sample_values": [1, 2]},
            {"name": "customer_id", "data_type": "IntegerType", "description": "Customer ID", "sample_values": [101, 102]},
            {"name": "order_date", "data_type": "DateTimeType", "description": "Order Date", "sample_values": ["2023-01-01", "2023-01-02"]},
            {"name": "total_amount", "data_type": "DecimalType", "description": "Total Amount", "sample_values": [150.75, 200.5]}
        ],
        description = "Order information"
    """
    print(f"Creating {layer_name} table {table_name} with columns {columns} and description {description}")
    stage_tables.append({"layer_name":layer_name, "table_name": table_name, description: description, "columns": columns})
    return f"Stage table {table_name} created successfully on {layer_name} layer."

@tool
def dimensional_design_principles():
    """Execute first always, when there is a request about to design a new table, column or KPI."""
    return (
        "The prefix for stage table is always 'stg_'."
        " The prefix for dimension table is always 'dim_'."
        " The prefix for fact table is always 'fact_'."
        " The primary key is always 'id'."
        " The stage table always has the same columns as the source table."
        " The dimension table includes the primary key and descriptive columns."
        " The KPI is always a column in a fact table."
        " All foreign keys in fact tables should reference the primary key of dimension tables."
        " Every source table should have a corresponding stage table."
)


def handle_tool_error(state) -> dict:
    error = state.get("error")
    tool_calls = state["messages"][-1].tool_calls
    return {
        "messages": [
            ToolMessage(
                content=f"Error: {repr(error)}\n please fix your mistakes.",
                tool_call_id=tc["id"],
            )
            for tc in tool_calls
        ]
    }


def create_tool_node_with_fallback(tools: list) -> dict:
    return ToolNode(tools).with_fallbacks(
        [RunnableLambda(handle_tool_error)], exception_key="error"
    )


def _print_event(event: dict, _printed: set, max_length=1500):
    current_state = event.get("dialog_state")
    if current_state:
        print("Currently in: ", current_state[-1])
    message = event.get("messages")
    if message:
        if isinstance(message, list):
            message = message[-1]
        if message.id not in _printed:
            msg_repr = message.pretty_repr(html=True)
            if len(msg_repr) > max_length:
                msg_repr = msg_repr[:max_length] + " ... (truncated)"
                if(msg_repr in "The prefix "):
                    msg_repr = msg_repr[:200] + " ... (truncated)"
            print(msg_repr)
            _printed.add(message.id)




class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


class Assistant:
    def __init__(self, runnable: Runnable):
        self.runnable = runnable

    def __call__(self, state: State, config: RunnableConfig):
        while True:
            configuration = config.get("configurable", {})
            user_id = configuration.get("passenger_id", None)
            state = {**state, "user_info": user_id}
            result = self.runnable.invoke(state)
            # If the LLM happens to return an empty response, we will re-prompt it
            # for an actual response.
            if not result.tool_calls and (
                not result.content
                or isinstance(result.content, list)
                and not result.content[0].get("text")
            ):
                messages = state["messages"] + [("user", "Respond with a real output.")]
                state = {**state, "messages": messages}
            else:
                break
        return {"messages": result}


llm = ChatOpenAI(model="gpt-4o-mini")

primary_assistant_prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are a helpful customer support assistant for Dimensional Data Modeling. "
            " Use the provided tools to search for tables and columns to answer the user's questions. "
            " If a search comes up empty, expand your search before giving up."
        ),
        ("placeholder", "{messages}"),
    ]
)


datamodel_tools = [
    get_source_tables_with_columns,
    get_stage_tables_with_columns,
    get_dimension_and_facts_tables_with_columns,
    dimensional_design_principles,
    add_table_with_columns,
]
datamodel_assistant_runnable = primary_assistant_prompt | llm.bind_tools(datamodel_tools)

from langchain_core.messages import AIMessage

builder = StateGraph(State)
builder.add_node("assistant", Assistant(datamodel_assistant_runnable))

builder.add_node("tools", create_tool_node_with_fallback(datamodel_tools))
builder.add_edge(START, "assistant")

builder.add_conditional_edges(
    "assistant",
    tools_condition,
)
builder.add_edge("tools", "assistant")

memory = MemorySaver()
part_1_graph = builder.compile(checkpointer=memory)

tutorial_questions = [
    # "Hi there, what stage tables do I have?",
    # # "What columns are in the table where I store the inventory information?",
    # "Create a new stage table for the Orders.",
    # "What stage tables do I have?",
    "I would like a KPI for the total amount of orders by date and customer. Create the missing tables!",
    # "Show me the dimensional tables.",
    # "Show me the fact tables.",
    # "I would like a KPI for number of sold products by customer. Create the missing tables!",
]

thread_id = str(uuid.uuid4())

config = {
    "configurable": {
        "thread_id": thread_id,
    }
}

_printed = set()
for question in tutorial_questions:
    events = part_1_graph.stream(
        {"messages": ("user", question)}, config, stream_mode="values"
    )
    for event in events:
        _print_event(event, _printed)