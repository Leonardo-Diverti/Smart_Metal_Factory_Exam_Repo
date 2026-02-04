import time
from collections import deque

class ControlPolicy:
    """
    Definisce la policy di gestione.
    - threshold: Soglia di attivazione.
    - restore_time: Tempo di ripristino (0 per azioni immediate come POST).
    - target_rt: Il Resource Type del target (attuatore o il sensore stesso).
    - method: "PUT" (accendi/spegni) o "POST" (esegui azione/reset).
    """
    def __init__(self, threshold, restore_time, target_rt, method="PUT"):
        self.threshold = threshold
        self.restore_time = restore_time
        self.target_rt = target_rt
        self.method = method

class SmartDevice:
    def __init__(self, name, resource_type, unit):
        self.name = name
        self.resource_type = resource_type
        self.unit = unit
        self.uri = None
        self.value = 0.0
        self.last_update = 0
        
        # Due livelli di policy: Normale (es. Compattazione) e Critica (es. Svuotamento)
        self.policy = None          
        self.critical_policy = None 
        
        self.actuation_in_progress = False
        self.history = deque(maxlen=20)

    def set_policy(self, policy):
        self.policy = policy

    def set_critical_policy(self, policy):
        self.critical_policy = policy

    def update_value(self, new_value):
        self.value = new_value
        self.last_update = time.time()
        self.history.append((self.last_update, new_value))

    def calculate_efficiency_rate(self):
        if len(self.history) < 2: return 0.0
        t_start, v_start = self.history[0]
        t_end, v_end = self.history[-1]
        time_diff = t_end - t_start
        if time_diff == 0: return 0.0
        val_diff = v_end - v_start
        return (val_diff / time_diff) * 60

class FactoryLocation:
    def __init__(self, department, island_name):
        self.department = department
        self.island_name = island_name
        self.devices = []

    def add_device(self, device):
        self.devices.append(device)

    def get_full_name(self):
        return f"{self.department} - {self.island_name}"