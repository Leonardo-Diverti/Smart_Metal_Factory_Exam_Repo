import json
import time

class FilterPumpActuatorDescriptor:
    def __init__(self):
        self.status = "OFF"
        self.last_activation = 0
        self.flow_rate_lpm = 0.0

    def set_status(self, status):
        self.status = status
        self.last_activation = int(time.time())

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)