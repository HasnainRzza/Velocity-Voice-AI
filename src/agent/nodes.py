import re
from typing import TypedDict, List, Annotated, Optional
from langgraph.graph import StateGraph, END

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool

from .llm import get_llm, SYSTEM_PROMPT
from .retriever import SimpleRetriever
from services.tts import DeepgramTTS

# Define State
class AgentState(TypedDict):
    messages: List[any]
    websocket: any # WebSocket connection to stream to
    tts: DeepgramTTS # TTS Service
    session: any # The current user session

retriever = SimpleRetriever(top_k=4)
llm = get_llm()

@tool
async def search_inventory(
    query: str = "",
    price_max: Optional[float] = None,
    car_name: Optional[str] = None,
) -> str:
    """Search the Genesis CPO inventory for vehicles.

    Args:
        query:     Descriptive text for semantic search. ALWAYS provide a meaningful
                   query — e.g. "low mileage", "fuel efficient", "luxury SUV",
                   "GV80 Royal", "automatic transmission". Use "car" as fallback only.
        price_max: Maximum price in SAR. Set ONLY when the user mentions a budget.
        car_name:  Specific Genesis model name (e.g. "GV80", "G90"). Set ONLY
                   when the user names a specific model.

    Returns:
        A list of matching vehicles with their names, trims, and prices.
        You MUST read and quote the exact car names from this result in your reply.
    """
    results = await retriever.retrieve(query, price_max, car_name)

    if not results:
        return "No matching cars found in the inventory."

    # Check if the retriever signaled that the requested model is not in stock
    # (not_found=True means these are semantic fallback / alternative results)
    not_found = results[0].get("not_found", False)
    searched_for = results[0].get("searched_for", car_name)

    docs = []
    for r in results:
        docs.append(f"- {r['document']} (Price: {r['metadata'].get('price', 'N/A')}, Name: {r['metadata'].get('name', 'N/A')})")

    inventory_text = "\n".join(docs)

    if not_found and searched_for:
        return (
            f"The model '{searched_for}' is not currently available in our inventory. "
            f"However, here are our closest available Genesis models that may interest you:\n{inventory_text}"
        )

    return inventory_text

llm_with_tools = llm.bind_tools([search_inventory])

def is_booking_intent(text: str) -> bool:
    patterns = [r"how do i book", r"place an order", r"reserve this", r"how can i purchase", r"i want to buy"]
    return any(re.search(p, text.lower()) for p in patterns)

def is_farewell_intent(text: str) -> bool:
    patterns = [r"^bye", r"goodbye", r"thank you that's all", r"end the call", r"exit", r"^stop", r"that's everything", r"see you later"]
    return any(re.search(p, text.lower()) for p in patterns)

async def intent_router(state: AgentState):
    """Determine intent immediately from the last user message."""
    last_msg = state["messages"][-1].content
    if is_booking_intent(last_msg):
        return "booking"
    if is_farewell_intent(last_msg):
        return "farewell"
    return "general"

async def booking_node(state: AgentState):
    """Handle booking intent without LLM generation."""
    response = "You can place your order through our official website. Once you submit your request, our team will contact you shortly to assist you with the purchase."
    await state["tts"].stream_sentence(response, state["websocket"], stop_event=state["session"].tts_stop_event)

    messages = state["messages"]
    messages.append(AIMessage(content=response))
    return {"messages": messages}

async def farewell_node(state: AgentState):
    """Handle farewell intent and close connection."""
    response = "Thank you for considering Genesis. Have a wonderful day!"
    await state["tts"].stream_sentence(response, state["websocket"], stop_event=state["session"].tts_stop_event)

    messages = state["messages"]
    messages.append(AIMessage(content=response))

    # Send a special signal that connection should close
    await state["websocket"].close(code=1000, reason="Farewell")
    return {"messages": messages}

async def generate_node(state: AgentState):
    """Invoke the LLM, collect the full response, then send it to TTS in one shot."""
    messages = state["messages"]
    session = state["session"]

    # Reset interrupt flag before generation starts
    session.is_interrupted = False

    # Get the complete response in one call (no streaming)
    response = await llm_with_tools.ainvoke(messages)

    # If the model wants to call a tool, hand off immediately
    if hasattr(response, "tool_calls") and response.tool_calls:
        messages.append(response)
        return {"messages": messages}

    full_response = response.content or ""
    messages.append(AIMessage(content=full_response))

    # Send the complete text to TTS only if there is content and session is active
    if full_response.strip() and not session.is_interrupted:
        await state["tts"].stream_sentence(
            full_response.strip(),
            state["websocket"],
            stop_event=session.tts_stop_event,
        )

    return {"messages": messages}


async def retrieve_node(state: AgentState):
    """Handle tool execution."""
    messages = state["messages"]
    last_msg = messages[-1]
    
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        # Inform user we are looking
        await state["tts"].stream_sentence(
            "Let me check our inventory for you.",
            state["websocket"],
            stop_event=state["session"].tts_stop_event,
        )
        
        for tool_call in last_msg.tool_calls:
            if tool_call["name"] == "search_inventory":
                args = tool_call["args"]
                result_str = await search_inventory.ainvoke(args)
                messages.append(ToolMessage(content=result_str, tool_call_id=tool_call["id"]))
                
    return {"messages": messages}

def should_retrieve(state: AgentState):
    last_msg = state["messages"][-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "retrieve"
    return END
