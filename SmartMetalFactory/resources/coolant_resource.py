import aiocoap.resource as resource
import aiocoap
import aiocoap.numbers as numbers
import time
from model.coolant_sensor import CoolantSensorDescriptor
from kpn_senml import *
from aiocoap.numbers.codes import Code

class CoolantResource(resource.ObservableResource):
    def __init__(self, device_name):
        super().__init__()
        self.device_name = device_name
        self.sensor = CoolantSensorDescriptor()
        self.if_ = "core.s" 
        self.rt = "it.unimore.device.sensor.coolant_turbidity"
        self.ct = numbers.media_types_rev['application/senml+json']
        self.title = "Coolant Turbidity"

    async def render_get(self, request):
        print(f"[SERVER] {self.device_name}: {self.sensor.value:.1f} NTU")
        
        pack = SenmlPack(self.device_name)
        pack.add(SenmlRecord("turbidity", unit="NTU", value=self.sensor.value, time=int(time.time())))
        return aiocoap.Message(content_format=numbers.media_types_rev['application/senml+json'], payload=pack.to_json().encode('utf8'))
    
    async def render_post(self, request):
        print(f"[SERVER] POST Received on {self.device_name}: Replacing Coolant...")
        self.sensor.value = 5.0 
        return aiocoap.Message(code=Code.CHANGED, payload="Coolant Replaced".encode('utf8'))