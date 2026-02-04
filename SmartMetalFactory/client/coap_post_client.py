import logging
import asyncio
from aiocoap import *

logging.basicConfig(level=logging.INFO)

# --- CONFIGURAZIONE TARGET ---
# Puntare a un SENSORE che supporta il reset (bin, turbidity)


# Es: "tornitura/isola-1/waste/bin-1" -> Svuota il cassone
URI_PATH = "tornitura/isola-1/waste/bin-1"

async def main():
    context = await Context.create_client_context()

    request_uri = f'coap://127.0.0.1:5683/{URI_PATH}'
    
    print(f"Sending POST (Reset) to: {request_uri}")

    try:
        # La POST per il reset spesso è vuota, ma se serve payload si può aggiungere
        request = Message(code=POST, uri=request_uri)
        response = await context.request(request).response

        print(f"\nResponse Code: {response.code}")
        print(f"Response Payload: {response.payload.decode('utf-8')}")

    except Exception as e:
        print(f"Error executing POST: {e}")

if __name__ == "__main__":
    asyncio.run(main())