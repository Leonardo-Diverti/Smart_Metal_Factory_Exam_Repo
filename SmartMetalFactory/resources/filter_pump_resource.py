import aiocoap.resource as resource
import aiocoap
import json
from model.filter_pump_actuator import FilterPumpActuatorDescriptor
from aiocoap.numbers.codes import Code

class FilterPumpResource(resource.ObservableResource):
    def __init__(self, device_name):
        super().__init__()
        self.device_name = device_name
        self.actuator = FilterPumpActuatorDescriptor()
        self.if_ = "core.a" 
        self.rt = "it.unimore.device.actuator.filter_pump"
        self.title = "Filter Pump Actuator"

    async def render_get(self, request):
        payload = self.actuator.to_json()
        return aiocoap.Message(payload=payload.encode('utf-8'))

    async def render_put(self, request):
        try:
            payload_string = request.payload.decode('utf-8')
            payload_dict = json.loads(payload_string)
            new_status = payload_dict['status']
            self.actuator.set_status(new_status)
            
            print(f"[SERVER] {self.device_name} set to {new_status}")
            
            return aiocoap.Message(code=Code.CHANGED, payload=self.actuator.to_json().encode('utf-8'))
        except Exception as e:
            print(f"[SERVER] Error on {self.device_name}: {e}")
            return aiocoap.Message(code=Code.BAD_REQUEST)