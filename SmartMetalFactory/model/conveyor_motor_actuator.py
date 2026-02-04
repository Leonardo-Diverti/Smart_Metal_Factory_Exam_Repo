import json
import time

class ConveyorMotorActuatorDescriptor:
    def __init__(self):
        self.status = "OFF"  # Default SPENTO per permettere l'accumulo
        self.last_update = int(time.time())
        self.speed_rpm = 0

    def set_status(self, status):
        self.status = status
        self.last_update = int(time.time())

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)