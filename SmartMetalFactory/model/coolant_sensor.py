import random
import json
import time

class CoolantSensorDescriptor:
    def __init__(self):
        self.value = 5.0 
        self.unit = "NTU"
        self.last_update = time.time()
        self.linked_actuator = None
        self.first_run = True # indica che è prima esecuzione

    def measure_turbidity(self):
        now = time.time()
        
        if self.first_run:
            elapsed_time = 0.0
            self.first_run = False
        else:
            elapsed_time = now - self.last_update
            
        self.last_update = now

        if self.linked_actuator and self.linked_actuator.status == "ON":
            # Pulizia (5.0 NTU al sec)
            clean_rate = 5.0
            self.value -= clean_rate * elapsed_time
            if self.value < 5.0: self.value = 5.0
        else:
            # Sporcamento Standard (0.3 - 0.8 NTU al sec)
            dirty_rate = random.uniform(0.3, 0.8)
            self.value += dirty_rate * elapsed_time
            
    def to_json(self):
        data = self.__dict__.copy()
        if 'linked_actuator' in data: del data['linked_actuator']
        return json.dumps(data)