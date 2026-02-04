import logging
import asyncio
import json
import link_header
from aiocoap import *
from smart_factory_data_model import SmartDevice, FactoryLocation, ControlPolicy

#definisce come appaiono messaggi su console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S') 

# COSTANTI RISORSE
RT_BIN = "it.unimore.device.sensor.bin_level"
RT_COMPACTOR = "it.unimore.device.actuator.compactor"
RT_TURBIDITY = "it.unimore.device.sensor.coolant_turbidity"
RT_PUMP = "it.unimore.device.actuator.filter_pump"
RT_WEIGHT = "it.unimore.device.sensor.conveyor_weight"
RT_MOTOR = "it.unimore.device.actuator.conveyor_motor"

TARGET = 'coap://127.0.0.1:5683'
WELL_KNOWN = "/.well-known/core"

class SmartWasteManager: #legge i dati sulla rete e applica politiche di controllo
    def __init__(self):
        self.context = None #registreremo contesto CoAP
        self.locations = [] #ci salveremo le isole create

    def add_device_chain(self, loc, type_key, index):
        suffix_name = f" {index}"
        suffix_id = f"-{index}"
        
        if type_key == 'bin':
            dev = SmartDevice(f"Waste Bin{suffix_name}", RT_BIN, "%")
            dev.internal_id = f"bin{suffix_id}"
            # Policy normale (Compactor)
            p_norm = ControlPolicy(threshold=80.0, restore_time=5.0, target_rt=RT_COMPACTOR, method="PUT")
            # Policy Critica (Emptying)
            p_crit = ControlPolicy(threshold=95.0, restore_time=0.0, target_rt=RT_BIN, method="POST")
            dev.set_policy(p_norm)
            dev.set_critical_policy(p_crit)
            loc.add_device(dev)

        elif type_key == 'coolant':
            dev = SmartDevice(f"Coolant Tank{suffix_name}", RT_TURBIDITY, "NTU")
            dev.internal_id = f"turbidity{suffix_id}"
            p_pol = ControlPolicy(threshold=15.0, restore_time=4.0, target_rt=RT_PUMP)
            dev.set_policy(p_pol)
            loc.add_device(dev)

        elif type_key == 'conveyor':
            dev = SmartDevice(f"Conveyor Belt{suffix_name}", RT_WEIGHT, "kg")
            dev.internal_id = f"weight{suffix_id}"
            p_pol = ControlPolicy(threshold=150.0, restore_time=3.0, target_rt=RT_MOTOR)
            dev.set_policy(p_pol)
            loc.add_device(dev)

    def create_island_structure(self, dept, name, config):
        loc = FactoryLocation(dept, name)
        for i in range(1, config.get('bin', 0) + 1):
            self.add_device_chain(loc, 'bin', i)
        for i in range(1, config.get('coolant', 0) + 1):
            self.add_device_chain(loc, 'coolant', i)
        for i in range(1, config.get('conveyor', 0) + 1):
            self.add_device_chain(loc, 'conveyor', i)
        return loc

    def setup_topology(self):
        
        config_isola_1 = {'bin': 3, 'coolant': 2, 'conveyor': 1}
        self.locations.append(self.create_island_structure("Reparto Tornitura", "isola-1", config_isola_1))

        config_isola_2 = {'bin': 1, 'coolant': 2, 'conveyor': 3}
        self.locations.append(self.create_island_structure("Reparto Tornitura", "isola-2", config_isola_2))

    async def discovery_phase(self):
        self.context = await Context.create_client_context()
        try:
            print(f"Discovering devices at {TARGET}...")
            req = Message(code=GET, uri=TARGET + WELL_KNOWN) #request a .well-knwn/core
            resp = await self.context.request(req).response #attendo risporta
            links = link_header.parse(resp.payload.decode('utf-8')).links #risposta in link format che viene tradotto da link header in oggetti facili da leggere
            
            for link in links:
                if 'rt' in dict(link.attr_pairs):
                    #viene estratto resource type e uri
                    found_rt = dict(link.attr_pairs)['rt']
                    found_uri = TARGET + link.href
                    
                    for loc in self.locations:
                        # matching tra oggetti virtuali vuoti (in loc.devices ci sono solo sensori) creati in setup_topology e risorse scoperte
                        if loc.island_name in found_uri:
                            for device in loc.devices:
                                if device.resource_type == found_rt and getattr(device, 'internal_id', '') in found_uri:
                                    device.uri = found_uri
                                    print(f"[MAPPED] {loc.island_name} > {device.name} -> {found_uri}")
                            
                            #matching degli attuatori collegati ai sensori
                            if "actuator" in found_rt:
                                if not hasattr(loc, "actuator_map"): loc.actuator_map = {} #inserimento in dizionario attuatori se non vi è ancora
                                try:
                                    idx_suffix = found_uri.split('-')[-1] #estrsggo il suffisso dell'attuatore
                                    key = f"{found_rt}-{idx_suffix}" #creo chiave per dizionario
                                    loc.actuator_map[key] = found_uri #resource tipe + suffisso che punta all'uri dell'attuatore
                                except: pass

        except Exception as e:
            print(f"Discovery Error: {e}")

    async def execute_policy(self, device, loc, policy):
        target_uri = None

        #caso A: il target è lo stesso tipo di risorsa del dispositivo (es. svuotamento cassone)
        if policy.target_rt == device.resource_type:
            target_uri = device.uri
        elif hasattr(loc, "actuator_map"): #il dispositivo agisce su un attuatore collegato
            dev_idx = device.internal_id.split('-')[-1] #estraggo suffisso dispositivo e creo chiave per cercare l'attuatore associato nella mappa degli attuatori
            lookup_key = f"{policy.target_rt}-{dev_idx}"
            target_uri = loc.actuator_map.get(lookup_key)
        
        if not target_uri: return

        device.actuation_in_progress = True #segna che l'attuatore è acceso e di non riattivarlo di nuovo finché non finisce 
        try:
            #POST e PUT per attuatori
            if policy.method == "POST":
                logging.warning(f"[{loc.island_name}] !!! CRITICAL {device.name} ({device.value:.1f}{device.unit}). RESET (POST)...")
                #creazione e invio messaggio POST
                req = Message(code=POST, uri=target_uri)
                await self.context.request(req).response
                logging.info(f"   -> [{loc.island_name}] {device.name}: FULLY EMPTIED.")

            elif policy.method == "PUT":
                logging.warning(f"[{loc.island_name}] !!! THRESHOLD {device.name} ({device.value:.1f}{device.unit}). ACTIVATION...")
                req_on = Message(code=PUT, uri=target_uri, payload=json.dumps({"status": "ON"}).encode('utf-8'))
                await self.context.request(req_on).response
                
                await asyncio.sleep(policy.restore_time) #5 secondi di ripristino in cui attuatore è ativo
                
                req_off = Message(code=PUT, uri=target_uri, payload=json.dumps({"status": "OFF"}).encode('utf-8'))
                await self.context.request(req_off).response
                logging.info(f"   -> [{loc.island_name}] {device.name}: RESTORED (OFF)")

        except Exception as e:
            logging.error(f"Policy Error: {e}")
        finally:
            device.actuation_in_progress = False

    async def run(self):
        print("--- STARTING SMART WASTE MANAGER ---")
        self.setup_topology() #creo isole e dispositivi virtuali
        await self.discovery_phase() #scoperta risorse CoAP in rete 

        while True:
            print("\n--- CYCLIC MONITORING (Every 1s) ---")
            for loc in self.locations:
                for device in loc.devices:
                    if not device.uri: continue
                    try:
                        req = Message(code=GET, uri=device.uri)
                        resp = await self.context.request(req).response 
                        data = json.loads(resp.payload.decode('utf-8'))
                        
                        if isinstance(data, list): val = data[0]['v'] #format SenML se dato è una lista quindi è di un sensore
                        else: val = float(data.get('v', 0)) # altrimenti fa parte del dizionario (attuatori) e prende solo valore v
                        
                        device.update_value(val) #riempie il valore nel dispostivo virtuale (sensore o attuatore che sia)
                        rate = device.calculate_efficiency_rate()
                        # Output su console del valore letto e tasso di variazione
                        print(f"   [{loc.island_name}] {device.name}: {val:.1f} {device.unit} | Rate: {rate:+.2f} {device.unit}/min")

                        if not device.actuation_in_progress: #se l'atttuatore non è già in funzione attiva le policy di controllo
                            if device.critical_policy and val >= device.critical_policy.threshold:
                                asyncio.create_task(self.execute_policy(device, loc, device.critical_policy))
                            elif device.policy and val > device.policy.threshold:
                                asyncio.create_task(self.execute_policy(device, loc, device.policy))
                                
                    except Exception as e:
                        logging.error(f"Read Error {device.name}: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    manager = SmartWasteManager()
    asyncio.run(manager.run())