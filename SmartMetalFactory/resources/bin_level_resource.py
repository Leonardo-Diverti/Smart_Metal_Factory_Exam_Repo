import aiocoap.resource as resource
import aiocoap
import aiocoap.numbers as numbers
import time
from model.bin_level_sensor import BinLevelSensorDescriptor
from kpn_senml import * #libreria per senML
from aiocoap.numbers.codes import Code #Status Code

class BinLevelResource(resource.ObservableResource): #classe risorsa sensore
    def __init__(self, device_name):
        super().__init__()
        self.device_name = device_name # Es: "isola-1-bin-2"
        self.sensor = BinLevelSensorDescriptor() #importo logica sensore
        self.if_ = "core.s" 
        self.rt = "it.unimore.device.sensor.bin_level"
        self.ct = numbers.media_types_rev['application/senml+json']
        self.title = "Bin Level Sensor"

    #ragionamento per rispondere a GET
    async def render_get(self, request):
        print(f"[SERVER] {self.device_name}: {self.sensor.value:.1f}%") #riporto nome e valore sul server per debug
        
        #creo pack SenML restituibile con GET (uso SenML perché si tratta di dati di un sensore)
        pack = SenmlPack(self.device_name)
        pack.add(SenmlRecord("waste", unit="%", value=self.sensor.value, time=int(time.time())))
        return aiocoap.Message(content_format=numbers.media_types_rev['application/senml+json'], payload=pack.to_json().encode('utf8')) #payload più digeribile da rete in bit
    
    #ragionamento per rispondere a POST
    async def render_post(self, request):
        print(f"[SERVER] POST Received on {self.device_name}: Emptying Bin...")
        self.sensor.value = 0.0
        return aiocoap.Message(code=Code.CHANGED, payload="Bin Emptied".encode('utf8')) #messaggio conferma
    
