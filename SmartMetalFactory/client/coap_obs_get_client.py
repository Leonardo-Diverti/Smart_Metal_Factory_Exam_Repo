import logging
import asyncio
from aiocoap import *
import json

logging.basicConfig(level=logging.INFO)

# --- CONFIGURAZIONE TARGET ---
# Modifica qui l'indirizzo del sensore da osservare
# Esempio: "tornitura/isola-1/waste/bin-1"
URI_PATH = "tornitura/isola-1/waste/bin-1"

def observation_callback(response):
    """Questa funzione viene chiamata AUTOMATICAMENTE ogni volta che arriva un dato"""
    try:
        payload = response.payload.decode('utf-8')
        data = json.loads(payload)
        
        # Se è una lista SenML, la rendiamo leggibile
        if isinstance(data, list) and len(data) > 0:
             val = data[0].get('v', 0)
             unit = data[0].get('u', '')
             name = data[0].get('n', 'Sensor')
             print(f">> UPDATE: {name} = {val:.2f} {unit}")
        else:
             # Altrimenti stampiamo il JSON grezzo
             print(f">> UPDATE (Raw): {payload}")
             
    except Exception as e:
        print(f">> UPDATE (Bytes): {response.payload}")

async def main():
    
    context = await Context.create_client_context()

    # Usiamo esplicitamente 127.0.0.1
    request_uri = f'coap://127.0.0.1:5683/{URI_PATH}'
    print(f"--- INIZIO OSSERVAZIONE: {request_uri} ---")
    print("Premi CTRL+C per fermare.")

    # Creazione messaggio con opzione Observe=0 (Registrazione)
    request = Message(code=GET, uri=request_uri, observe=0)
    
    # Invio richiesta
    request_interface = context.request(request)
    
    # AGGANCIO LA CALLBACK
    request_interface.observation.register_callback(observation_callback)

    try:
        # Aspettiamo la prima risposta
        response = await request_interface.response
        print(f"Stato Iniziale: Connesso. In attesa di aggiornamenti...")
    except Exception as e:
        print(f"Errore di connessione: {e}")
        return

    # Loop infinito
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOsservazione terminata dall'utente.")