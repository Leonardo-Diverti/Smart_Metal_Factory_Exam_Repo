import random
import json #trasformo dati in formato json
import time

class BinLevelSensorDescriptor: #classe di creazione sensore livello cassone
    def __init__(self):
        self.value = 0.0 #valore iniziale cassone
        self.unit = "%" 
        self.last_update = time.time() #segno ora attuale
        self.linked_actuator = None #nessun attuatore collegato al momento
        self.compacted_floor = 0.0 
        self.first_run = True  #indica che è la prima esecuzione del sensore

    def measure_level(self):
        now = time.time() #ora attuale
        
        # Se è la prima esecuzione, azzeriamo il tempo trascorso in modo che non risulti un numero sballato visto che alla prima esecuzione  last_update vale 0
        if self.first_run:
            elapsed_time = 0.0
            self.first_run = False #non è più la prima esecuzione
        else:
            elapsed_time = now - self.last_update #calcolo tempo trascorso dall'ultimo aggiornamento
            
        self.last_update = now #aggiorno ultimo aggiornamento a ora attuale

        # Reset pavimento se valore azzerato (es. da POST)
        if self.value < self.compacted_floor:
            self.compacted_floor = self.value

        if self.linked_actuator and self.linked_actuator.status == "ON": # mi assicuro che sia collegato un attuatore e che esso sia su ON
            if self.value > self.compacted_floor:
                compression_speed = 15.0 
                self.value -= compression_speed * elapsed_time # diminuisco il valore del cassone in base al tempo trascorso
                
                # Crescita pavimento
                floor_growth = compression_speed * 0.3
                self.compacted_floor += floor_growth * elapsed_time

                if self.value < self.compacted_floor:
                    self.value = self.compacted_floor
            else:
                self.value = self.compacted_floor #condizione diversa da spra perché qua pressa è accesa
        else:
            # Riempimento Standard (3.0% - 5.0% al sec)
            fill_rate = random.uniform(3.0, 5.0) 
            increase = fill_rate * elapsed_time
            self.value += increase
            if self.value > 100.0: self.value = 100.0

    def to_json(self): 
        data = self.__dict__.copy() #copio variabili self. dellla claasse in un dizionario
        if 'linked_actuator' in data: del data['linked_actuator'] #tolgo collegamento attuatore perché non trasformabile in json
        return json.dumps(data) #trasformo dizionario inn json