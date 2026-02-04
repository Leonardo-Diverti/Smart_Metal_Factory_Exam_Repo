import logging
import asyncio
from aiocoap import *
import json

logging.basicConfig(level=logging.INFO)

# --- CONFIGURAZIONE ---
# Assicurati di puntare a un ATTUATORE (compactor, pump, motor)
# Es: "tornitura/isola-1/conveyor/motor-1"
URI_PATH = "tornitura/isola-1/conveyor/motor-1"

# Scegli azione: "ON" oppure "OFF"
CMD_STATUS = "ON"

async def main():
    # 1. Creiamo il contesto in modo standard (senza 'bind' che dava errore)
    context = await Context.create_client_context()

    # Usiamo esplicitamente 127.0.0.1
    request_uri = f'coap://127.0.0.1:5683/{URI_PATH}'
    
    # 2. FIX FONDAMENTALE: Creiamo un JSON perfetto
    # Questo risolve l'errore "4.00 Bad Request"
    payload_dict = {"status": CMD_STATUS}
    payload_bytes = json.dumps(payload_dict).encode('utf-8')

    print(f"--- INVIO COMANDO PUT ---")
    print(f"Target: {request_uri}")
    print(f"Payload: {payload_dict}")

    try:
        # Creazione messaggio PUT con il payload JSON
        request = Message(code=PUT, uri=request_uri, payload=payload_bytes)
        
        # Invio
        response = await context.request(request).response

        print(f"\n--- RISPOSTA ---")
        print(f"Code: {response.code}")
        
        # Se vedi ancora l'avviso "different address", ignoralo se il codice è 2.04
        if response.code.is_successful():
            print("Successo! Attuatore aggiornato.")
        else:
            print(f"Errore: Il server ha rifiutato il comando (Codice {response.code}).")

    except Exception as e:
        print(f"Errore di comunicazione: {e}")

if __name__ == "__main__":
    asyncio.run(main())