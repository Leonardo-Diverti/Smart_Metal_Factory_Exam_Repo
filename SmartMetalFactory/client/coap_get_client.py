import logging
import asyncio
from aiocoap import *
import json

logging.basicConfig(level=logging.INFO)

# --- CONFIGURAZIONE TARGET ---
# Modifica qui per cambiare dispositivo (es. bin-2, isola-2, ecc.)
# Esempi:
# "tornitura/isola-1/waste/bin-1"
# "tornitura/isola-1/conveyor/weight-2"
URI_PATH = "tornitura/isola-1/waste/bin-1"

async def main():
    protocol = await Context.create_client_context()

    request_uri = f'coap://127.0.0.1:5683/{URI_PATH}'
    print(f"Requesting GET to: {request_uri}")

    try:
        request = Message(code=GET, uri=request_uri)
        response = await protocol.request(request).response

        print("\n--- RISPOSTA RICEVUTA ---")
        print(f"Code: {response.code}")
        
        # Tentativo di decodifica JSON per lettura pulita
        try:
            payload_str = response.payload.decode('utf-8')
            data = json.loads(payload_str)
            print(f"Payload (JSON): {json.dumps(data, indent=2)}")
            
            # Se è formato SenML (lista), estrai il valore
            if isinstance(data, list) and 'v' in data[0]:
                print(f"VALORE ESTRATTO: {data[0]['v']} {data[0].get('u', '')}")
            # Se è un attuatore (dict), estrai lo status
            elif isinstance(data, dict) and 'status' in data:
                print(f"STATO ATTUATORE: {data['status']}")
                
        except:
            print(f"Payload (Raw): {response.payload}")

    except Exception as e:
        print(f"Failed to fetch resource: {e}")

if __name__ == "__main__":
    asyncio.run(main())