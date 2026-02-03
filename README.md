# Smart_Metal_Factory_Exam_Repository

# Table of Contents

- [Scenario - Smart Metal Factory](#scenario---smart-metal-factory)
  - [High-Level Architecture & Main Components](#high-level-architecture--main-components)
  - [Data Models](#data-models)
  - [Protocols & Communication](#protocols--communication)
    - [CoAP Resources & Mapping](#coap-resources--mapping)
    - [Interaction Patterns](#interaction-patterns)
- [Project Structure](#project-structure)
- [How to Start](#how-to-start)
  - [Prerequisites](#prerequisites)
  - [Running the Server](#running-the-server)
  - [Running the Manager](#running-the-manager)

---

# Scenario - Smart Metal Factory

Il progetto **Smart Metal Factory** simula un ambiente industriale digitalizzato (Digital Twin) dedicato alla lavorazione dei metalli (es. Tornitura). L'obiettivo è monitorare e gestire in tempo reale lo stato delle isole produttive, ottimizzando la gestione degli scarti, la qualità del refrigerante e il flusso dei materiali sui nastri trasportatori.

Il sistema è composto da un **Server CoAP** che simula la fisica dei dispositivi (sensori e attuatori) e da uno **Smart Waste Manager** (Client) che agisce come controller intelligente, eseguendo policy di automazione basate sui dati di telemetria ricevuti.

## High-Level Architecture & Main Components

- **Edge/Server Layer (Factory Server)**
  Simula il comportamento fisico dei dispositivi e li espone come risorse CoAP. Include un "Physics Engine" interno che aggiorna lo stato dei sensori a 10Hz.
  - **Sensors:**
    - **Bin Level Sensor**: Misura la percentuale di riempimento dei contenitori di scarti metallici.
    - **Coolant Turbidity Sensor**: Monitora la torbidità del liquido refrigerante (in NTU) per prevenire malfunzionamenti.
    - **Conveyor Weight Sensor**: Rileva il peso del materiale (in kg) trasportato sui nastri.
  - **Actuators:**
    - **Compactor**: Comprime gli scarti nel bidone per guadagnare spazio.
    - **Filter Pump**: Attiva il filtraggio del refrigerante per ridurne la torbidità.
    - **Conveyor Motor**: Gestisce il movimento del nastro trasportatore.

- **Control Layer (Smart Waste Manager)**
  Un'applicazione client autonoma che:
  - Esegue la **Discovery** automatica delle risorse disponibili sulla rete.
  - Sottoscrive (Observe) le risorse per ricevere aggiornamenti in tempo reale.
  - Applica **Policy di Controllo** (es. Soglia 80% -> Attiva Compattatore; Soglia 95% -> Svuotamento Critico).
  - Calcola metriche di efficienza (es. rate di riempimento al minuto).

---

## Data Models

Ogni sensore e attuatore utilizza un modello dati specifico. Per la telemetria viene utilizzato prevalentemente il formato **SenML+JSON**, mentre per la configurazione degli attuatori si utilizzano oggetti JSON custom.

I timestamp sono rappresentati come Unix epoch time.

**Bin Level Data Model**

Progettato per il monitoraggio dei rifiuti di lavorazione.

| **Field** | **Type** | **Description** |
|-----------|----------|-----------------|
| `n` (name) | String | Identificativo risorsa (es. "waste") |
| `u` (unit) | String | Unità di misura ("%") |
| `v` (value)| Double | Livello di riempimento corrente (0.0 - 100.0) |
| `t` (time) | Long | Unix epoch timestamp (s) |

**Coolant Turbidity Data Model**

Progettato per il monitoraggio della qualità dei fluidi.

| **Field** | **Type** | **Description** |
|-----------|----------|-----------------|
| `n` (name) | String | Identificativo risorsa (es. "turbidity") |
| `u` (unit) | String | Unità di misura ("NTU" - Nephelometric Turbidity Units) |
| `v` (value)| Double | Valore di torbidità corrente (min 5.0) |
| `t` (time) | Long | Unix epoch timestamp (s) |

**Conveyor Weight Data Model**

Progettato per monitorare il carico sulla linea di trasporto.

| **Field** | **Type** | **Description** |
|-----------|----------|-----------------|
| `n` (name) | String | Identificativo risorsa (es. "weight") |
| `u` (unit) | String | Unità di misura ("kg") |
| `v` (value)| Double | Peso rilevato sul nastro |
| `t` (time) | Long | Unix epoch timestamp (s) |

**Actuator Status Data Model**

Utilizzato per inviare comandi (PUT) o leggere lo stato (GET) degli attuatori (Compactor, Pump, Motor).

| **Field** | **Type** | **Description** |
|-----------|----------|-----------------|
| `status` | String | Stato operativo: "ON" o "OFF" |
| `last_activation` | Long | Timestamp dell'ultima attivazione |
| `cycle_count` | Integer | (Opzionale) Numero di cicli eseguiti |

---

## Protocols & Communication

Il sistema si basa interamente sul protocollo **CoAP (Constrained Application Protocol)** (RFC 7252), ideale per ambienti IoT industriali con risorse limitate.

- **Transport Layer**: UDP (Porta standard 5683).
- **Format**: JSON / SenML+JSON.

### CoAP Resources & Mapping

Le risorse sono organizzate gerarchicamente per riflettere la topologia della fabbrica: `Reparto / Isola / Tipo / Dispositivo`.

| Resource URI Path | Interface (`if`) | Resource Type (`rt`) | Methods | Description |
|-------------------|------------------|----------------------|---------|-------------|
| `tornitura/isola-X/waste/bin-Y` | `core.s` | `it.unimore.device.sensor.bin_level` | GET (Obs), POST | GET: Livello attuale. POST: Reset (Svuotamento). |
| `tornitura/isola-X/waste/compactor-Y` | `core.a` | `it.unimore.device.actuator.compactor` | GET, PUT | PUT: Attiva/Disattiva (ON/OFF). |
| `tornitura/isola-X/coolant/turbidity-Y` | `core.s` | `it.unimore.device.sensor.coolant_turbidity` | GET (Obs), POST | GET: Torbidità. POST: Sostituzione liquido. |
| `tornitura/isola-X/coolant/pump-Y` | `core.a` | `it.unimore.device.actuator.filter_pump` | GET, PUT | PUT: Attiva/Disattiva pompa. |
| `tornitura/isola-X/conveyor/weight-Y` | `core.s` | `it.unimore.device.sensor.conveyor_weight` | GET (Obs) | GET: Peso rilevato. |
| `tornitura/isola-X/conveyor/motor-Y` | `core.a` | `it.unimore.device.actuator.conveyor_motor` | GET, PUT | PUT: Avvia/Ferma motore. |



