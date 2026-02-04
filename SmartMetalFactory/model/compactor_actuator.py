import json
import time

class CompactorActuatorDescriptor:
    def __init__(self):
        self.status = "OFF"
        self.last_activation = 0
        self.cycle_count = 0

    def set_status(self, status):
        self.status = status 
        self.last_activation = int(time.time()) #ora attuale per ultima attivazione
        if status == "ON": 
            self.cycle_count += 1

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__) #passo intero oggetto per tradurre dati in json