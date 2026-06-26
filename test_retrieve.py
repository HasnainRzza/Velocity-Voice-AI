import sys, asyncio
sys.path.append('d:/Projects/TechNext/voice-ai/src')
from agent.retriever import SimpleRetriever

async def run():
    r = SimpleRetriever()
    res = await r.retrieve('G90', car_name='G90')
    print([rx['metadata'].get('name') for rx in res])

asyncio.run(run())
