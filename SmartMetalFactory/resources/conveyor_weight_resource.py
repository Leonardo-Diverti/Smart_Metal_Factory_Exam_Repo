import aiocoap.resource as resource
import aiocoap
import aiocoap.numbers as numbers
import time
from model.conveyor_weight_sensor import ConveyorWeightSensorDescriptor
from kpn_senml import *

class ConveyorWeightResource(resource.ObservableResource):
    def __init__(self, device_name):
        super().__init__()
        self.device_name = device_name
        self.sensor = ConveyorWeightSensorDescriptor()
        self.if_ = "core.s" 
        self.rt = "it.unimore.device.sensor.conveyor_weight"
        self.ct = numbers.media_types_rev['application/senml+json']
        self.title = "Conveyor Weight Sensor"

    async def render_get(self, request):
        print(f"[SERVER] {self.device_name}: {self.sensor.value:.1f} kg")
        
        pack = SenmlPack(self.device_name)
        pack.add(SenmlRecord("weight", unit="kg", value=self.sensor.value, time=int(time.time())))
        return aiocoap.Message(content_format=numbers.media_types_rev['application/senml+json'], payload=pack.to_json().encode('utf8'))