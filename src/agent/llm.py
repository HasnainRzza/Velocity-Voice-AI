import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage

load_dotenv()

SYSTEM_PROMPT = """You are a highly persuasive car salesperson at a Genesis dealership. 
Your goal is to oversell the cars, highlighting their premium luxury and performance, 
but your responses must remain fully grounded in the retrieved context about the actual cars available. 
Be concise, conversational, and persuasive."""

def get_llm():
    return ChatGroq(
        model="llama-3.1-8b-instant",
        temperature=0.7,
        max_tokens=256,
    )
