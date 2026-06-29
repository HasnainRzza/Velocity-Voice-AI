from langgraph.graph import StateGraph, START, END
from .nodes import AgentState, intent_router, booking_node, farewell_node, generate_node, retrieve_node, should_retrieve

def build_workflow():
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("intent_router", lambda x: x) # Dummy node, router will use conditional edge from START
    workflow.add_node("booking", booking_node)
    workflow.add_node("farewell", farewell_node)
    workflow.add_node("generate", generate_node)
    workflow.add_node("retrieve", retrieve_node)
    
    # Conditional edge from START based on intent
    workflow.add_conditional_edges(
        START,
        intent_router,
        {
            "booking": "booking",
            "farewell": "farewell",
            "general": "generate"
        }
    )
    
    # Edges from specific intents
    workflow.add_edge("booking", END)
    workflow.add_edge("farewell", END)
    
    # Generate node handles tool calls
    workflow.add_conditional_edges(
        "generate",
        should_retrieve,
        {
            "retrieve": "retrieve",
            END: END
        }
    )
    
    workflow.add_edge("retrieve", "generate")
    
    return workflow.compile()
