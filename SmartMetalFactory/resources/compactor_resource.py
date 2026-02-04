import aiocoap.resource as resource
import aiocoap
import json
from model.compactor_actuator import CompactorActuatorDescriptor
from aiocoap.numbers.codes import Code

class CompactorResource(resource.ObservableResource):
    def __init__(self, device_name):
        super().__init__()
        self.device_name = device_name
        self.actuator = CompactorActuatorDescriptor()
        self.if_ = "core.a" 
        self.rt = "it.unimore.device.actuator.compactor"
        self.title = "Compactor Actuator"
 
    async def render_get(self, request): #restituisce stato attuale compattatore
        payload = self.actuator.to_json()
        return aiocoap.Message(payload=payload.encode('utf-8'))

    async def render_put(self, request): 
        try:
            payload_string = request.payload.decode('utf-8') #da byte a stringa
            payload_dict = json.loads(payload_string) #da stringa a dizionario
            new_status = payload_dict['status'] #estraggo nuovo sato
            self.actuator.set_status(new_status) #aggiorno status
            
            # Log specifico di modifica dello stato
            print(f"[SERVER] {self.device_name} set to {new_status}")
            
            #conferma al client di modifica
            return aiocoap.Message(code=Code.CHANGED, payload=self.actuator.to_json().encode('utf-8'))
        except Exception as e:
            print(f"[SERVER] Error on {self.device_name}: {e}") #log di errore
            return aiocoap.Message(code=Code.BAD_REQUEST)