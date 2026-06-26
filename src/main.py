import os
import threading
from typing import TypedDict, List, Annotated, Optional

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph, END
from pydantic import BaseModel, Field
from langchain_core.tools import tool

from agent.transcribe import Transcriber
from agent.retriever import SimpleRetriever
from agent.llm import get_llm, SYSTEM_PROMPT
from agent.tts import speak

# State Definition
class State(TypedDict):
    messages: List[any]
    
# Global instances for the hardware/services
interrupt_event = threading.Event()
transcriber = Transcriber(interrupt_event)
retriever = SimpleRetriever(top_k=4)
llm = get_llm()

# Tool definition for LangChain
@tool
def search_inventory(query: Optional[str] = None, price_max: Optional[float] = None, car_name: Optional[str] = None) -> str:
    """Search the Genesis inventory for cars matching the criteria. Use this to lookup cars before answering.
    
    Args:
        query: The search query (e.g., 'luxury SUV').
        price_max: Maximum price of the car.
        car_name: Specific name or model of the car (e.g., 'GV80').
    """
    # Ensure query is at least an empty string for the retriever
    query_str = query if query else ""
    results = retriever.retrieve(query_str, price_max=price_max, car_name=car_name)
    if not results:
        return "No matching cars found in the inventory."
    
    docs = []
    for r in results:
        docs.append(f"- {r['document']} (Price: {r['metadata'].get('price', 'N/A')}, Name: {r['metadata'].get('name', 'N/A')})")
    return "\n".join(docs)

llm_with_tools = llm.bind_tools([search_inventory])

# Nodes
def listen_node(state: State):
    print("\n[Agent is Listening...]")
    interrupt_event.clear()
    transcriber.set_ignore_mode(False)
    
    transcript = ""
    while not transcript:
        transcript = transcriber.get_transcript(timeout=0.5)
        
    print(f"\nUser: {transcript}")
    
    messages = state.get("messages", [])
    if not messages:
        messages = [SystemMessage(content=SYSTEM_PROMPT)]
        
    messages.append(HumanMessage(content=transcript))
    return {"messages": messages}

def think_node(state: State):
    print("\n[Agent is Thinking...]")
    messages = state["messages"]
    response = llm_with_tools.invoke(messages)
    messages.append(response)
    return {"messages": messages}

def retrieve_node(state: State):
    print("\n[Agent is Retrieving...]")
    # Tell user we are looking it up, and ignore interruptions
    transcriber.set_ignore_mode(True)
    msg = "Hang on a minute, let me check our inventory for you..."
    print(f"Agent: {msg}")
    speak(msg, interrupt_event)
    
    messages = state["messages"]
    last_msg = messages[-1]
    
    if not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
        return {"messages": messages}
        
    for tool_call in last_msg.tool_calls:
        if tool_call["name"] == "search_inventory":
            args = tool_call["args"]
            print(f"-> Searching: {args}")
            result_str = search_inventory.invoke(args)
            messages.append(ToolMessage(content=result_str, tool_call_id=tool_call["id"]))
            
    return {"messages": messages}

def speak_node(state: State):
    messages = state["messages"]
    last_msg = messages[-1]
    
    if isinstance(last_msg, AIMessage) and last_msg.content:
        text = last_msg.content
        print(f"\nAgent: {text}")
        
        # Clear interrupt event before speaking
        interrupt_event.clear()
        transcriber.set_ignore_mode(False)
        
        # Play the actual agent response (Streaming)
        speak(text, interrupt_event)
        
    return {"messages": messages}

def should_retrieve(state: State):
    last_msg = state["messages"][-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "retrieve"
    return "speak"

# Graph Construction
workflow = StateGraph(State)

workflow.add_node("listen", listen_node)
workflow.add_node("think", think_node)
workflow.add_node("retrieve", retrieve_node)
workflow.add_node("speak", speak_node)

# Set entry point to speak so it greets immediately!
workflow.set_entry_point("speak")

workflow.add_edge("listen", "think")
workflow.add_conditional_edges("think", should_retrieve)
workflow.add_edge("retrieve", "think")
workflow.add_edge("speak", "listen")

app = workflow.compile()

def main():
    print("Starting Conversational Agent...")
    transcriber.start()
    
    # Pre-seed the state with the greeting!
    initial_greeting = "Welcome to Genesis! How can I help you find your perfect car today?"
    state = {
        "messages": [
            SystemMessage(content=SYSTEM_PROMPT),
            AIMessage(content=initial_greeting)
        ]
    }
    
    try:
        while True:
            # We step through the graph. The graph is cyclic (speak -> listen -> think -> ...)
            state = app.invoke(state)
    except KeyboardInterrupt:
        print("\nStopping Agent...")
        transcriber.stop()

if __name__ == "__main__":
    main()
