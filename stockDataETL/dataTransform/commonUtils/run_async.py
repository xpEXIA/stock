import asyncio
import threading

def run_async(func):
    threading.Thread(target=asyncio.run, args=(func,)).start()