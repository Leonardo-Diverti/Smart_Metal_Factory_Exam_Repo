import random
import json
import time

class ConveyorWeightSensorDescriptor:
    def __init__(self):
        self.value = 0.0
        self.unit = "kg"
        self.last_update = time.time()
        self.linked_actuator = None
        self.first_run = True # indica che è la prima esecuzione del sensore

    def measure_weight(self):
        now = time.time()
        
        if self.first_run:
            elapsed_time = 0.0
            self.first_run = False
        else:
            elapsed_time = now - self.last_update
            
        self.last_update = now

        if self.linked_actuator and self.linked_actuator.status == "ON": #se esiste attuatore collegato e attivo
            # Scarico (25 kg al sec)
            unload_rate = 25.0
            self.value -= unload_rate * elapsed_time
            if self.value < 0: self.value = 0.0
        else:
            # Carico Standard (4.0 - 8.0 kg al sec)
            load_rate = random.uniform(4.0, 8.0)
            self.value += load_rate * elapsed_time
            if self.value > 200: self.value = 200.0

    def to_json(self):
        data = self.__dict__.copy()
        if 'linked_actuator' in data: del data['linked_actuator']
        return json.dumps(data)