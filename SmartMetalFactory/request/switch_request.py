import json

class SwitchRequestDescriptor: #definisce in maniera chiara i messaggi ON e OFF da usare in altri moduli
    STATUS_ON = "ON"
    STATUS_OFF = "OFF"

    def __init__(self, status):
        self.status = status

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)