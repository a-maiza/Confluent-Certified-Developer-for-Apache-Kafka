# Apache Kafka — Introduction aux Producers

## Table des matières
- [Module 1 : Les Producers Kafka](#module-1--les-producers-kafka)

---

## Module 1 : Les Producers Kafka

### Sujet

Ce module introduit le concept de **producer** dans Apache Kafka : ce qu'est un producer, comment il s'intègre dans l'écosystème Kafka, et comment il s'utilise concrètement via l'API Java.

---

### Concepts clés

#### Producer
Un **producer** est une application cliente qui écrit des données dans un cluster Kafka. Tout composant de la plateforme Kafka qui n'est pas un broker est, au fond, soit un producer, soit un consumer, soit les deux. Les producers sont le mécanisme par lequel on insère des données dans Kafka.

##### Schéma — Producer / Consumer / Broker dans l'écosystème

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P1["🏭 Producer A"] -->|"écrit"| K[("⚡ Cluster Kafka<br/>(brokers)")]
        P2["🏭 Producer B"] -->|"écrit"| K
        K -->|"lit"| C1["📱 Consumer X"]
        K -->|"lit"| C2["📱 Consumer Y"]

        BOTH["🔁 Application<br/>(producer + consumer)"]
        BOTH -->|"écrit"| K
        K -->|"lit"| BOTH
    end

    style P1 fill:#43a047,color:#fff
    style P2 fill:#43a047,color:#fff
    style C1 fill:#fb8c00,color:#fff
    style C2 fill:#fb8c00,color:#fff
    style BOTH fill:#7b1fa2,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### KafkaProducer
`KafkaProducer` est la classe principale de l'API Java permettant à une application de se connecter au cluster et d'y envoyer des messages. Elle gère en interne toute la plomberie réseau : connexion aux brokers, accusés de réception (acknowledgements), retransmissions en cas d'échec, idempotence forcée, batching pour optimiser le débit, etc.

##### Schéma — KafkaProducer : façade simple, plomberie complexe

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        APP["💻 Application Java"] --> API["☕ KafkaProducer<br/>(API simple)"]

        subgraph INTERNALS ["🔧 Plomberie interne"]
            direction LR
            I1["🔌 Connexion brokers"]
            I2["✅ Acknowledgements"]
            I3["🔁 Retransmissions"]
            I4["🛡️ Idempotence"]
            I5["📦 Batching"]
        end

        API --> INTERNALS
        INTERNALS --> K[("⚡ Cluster Kafka")]
    end

    style APP fill:#43a047,color:#fff
    style API fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:2px
    style INTERNALS fill:#fff8e1,stroke:#f57c00
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### ProducerRecord
`ProducerRecord` est la classe utilisée pour représenter un message à envoyer. Un message Kafka est une **paire clé-valeur**. Le `ProducerRecord` encapsule :
- la **clé** du message
- la **valeur** du message
- le **topic** de destination
- optionnellement : le **timestamp**, la **partition** cible, et des **headers** (paires clé-valeur supplémentaires)

##### Schéma — Anatomie d'un ProducerRecord

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        PR["📦 ProducerRecord"]
        PR --> T["📋 topic (obligatoire)"]
        PR --> K["🔑 key"]
        PR --> V["🎯 value"]
        PR --> OPT["⚙️ Optionnels"]
        OPT --> TS["⏱️ timestamp"]
        OPT --> P["📦 partition cible"]
        OPT --> H["🏷️ headers"]
    end

    style PR fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style T fill:#039be5,color:#fff
    style K fill:#fb8c00,color:#fff
    style V fill:#43a047,color:#fff
    style OPT fill:#9e9e9e,color:#fff
    style TS fill:#7b1fa2,color:#fff
    style P fill:#e53935,color:#fff
    style H fill:#5d4037,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Bootstrap Servers
Le paramètre `bootstrap.servers` est une liste de quelques brokers du cluster (deux ou trois suffisent). Il permet au producer de se connecter à au moins un broker pour récupérer les métadonnées complètes du cluster. Il est inutile — et déconseillé — de lister tous les brokers, car leur nombre peut être élevé (ex. 50) et certains peuvent être temporairement indisponibles.

##### Schéma — Bootstrap : un point d'entrée, puis découverte complète

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producer"] -->|"1️⃣ se connecte à<br/>bootstrap.servers<br/>(2-3 brokers suffisent)"| B1["🖥️ Broker 1"]
        B1 -->|"2️⃣ renvoie les métadonnées<br/>du cluster complet"| P
        P -->|"3️⃣ communique<br/>avec tous les brokers"| ALL[("🌐 Tous les brokers<br/>(ex. 50)")]
    end

    style P fill:#43a047,color:#fff
    style B1 fill:#fff8e1,stroke:#f57c00,stroke-width:2px
    style ALL fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Sérialisation
Kafka est agnostique au format de données : la clé et la valeur sont de simples **bytes**. Des sérialiseurs natifs existent pour les types primitifs (`Integer`, `Long`, `Double`, `String`). Pour des objets métier complexes, il est recommandé d'utiliser un format structuré (ex. Avro, Protobuf) avec le **Confluent Schema Registry**, abordé dans un module ultérieur du cours.

##### Schéma — Sérialisation : objet → bytes

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        OBJ["💻 Objet métier<br/>(ex: Reading)"] --> SER["🔧 Serializer"]
        SER --> NATIVES["📦 Natifs<br/>Integer / Long<br/>Double / String"]
        SER --> STRUCT["📋 Structurés<br/>+ Schema Registry<br/>(Avro / Protobuf)"]
        NATIVES --> BYTES["🔢 byte[]"]
        STRUCT --> BYTES
        BYTES --> K[("⚡ Kafka<br/>(agnostique)")]
    end

    style OBJ fill:#43a047,color:#fff
    style SER fill:#fb8c00,color:#fff
    style NATIVES fill:#fff8e1,stroke:#f57c00
    style STRUCT fill:#e1bee7,stroke:#6a1b9a
    style BYTES fill:#9e9e9e,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

#### Configuration du producer
Le `KafkaProducer` reçoit un ensemble de **paramètres de configuration** sous forme de paires clé-valeur (souvent via un fichier `properties`). Les paramètres fondamentaux sont :
- `bootstrap.servers` : liste des adresses de brokers pour l'initialisation de la connexion.
- `acks` : niveau d'accusé de réception souhaité (ex. `acks=all` pour une durabilité maximale).

En pratique, une configuration réelle comporte généralement entre 5 et 10 paramètres (sécurité, compression, timeouts, etc.).

##### Schéma — Configuration via paires clé-valeur

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        FILE["📄 producer.properties"]
        FILE --> CFG["⚙️ Configuration"]
        CFG --> C1["🔌 bootstrap.servers"]
        CFG --> C2["✅ acks"]
        CFG --> C3["🔐 sécurité"]
        CFG --> C4["🗜️ compression"]
        CFG --> C5["⏱️ timeouts"]
        CFG --> CN["... (5 à 10 params)"]

        CFG --> KP["☕ KafkaProducer"]
    end

    style FILE fill:#9e9e9e,color:#fff
    style CFG fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:2px
    style C1 fill:#fff8e1,stroke:#f57c00
    style C2 fill:#fff8e1,stroke:#f57c00
    style C3 fill:#fff8e1,stroke:#f57c00
    style C4 fill:#fff8e1,stroke:#f57c00
    style C5 fill:#fff8e1,stroke:#f57c00
    style CN fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:3 3
    style KP fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Envoi d'un message
1. Créer un objet `KafkaProducer` avec la configuration appropriée.
2. Créer un `ProducerRecord` contenant le topic, la clé et la valeur du message.
3. Appeler la méthode d'envoi du producer.

Sous le capot, le producer gère automatiquement les connexions, les retransmissions, et les accusés de réception.

##### Schéma — Envoi d'un message en 3 étapes

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant APP as 💻 Application
    participant KP as ☕ KafkaProducer
    participant PR as 📦 ProducerRecord
    participant K as ⚡ Cluster Kafka

    rect rgb(250, 250, 250)
        APP->>KP: 1️⃣ new KafkaProducer(config)
        APP->>PR: 2️⃣ new ProducerRecord(topic, key, value)
        APP->>KP: 3️⃣ producer.send(record)

        Note over KP,K: 🔧 Plomberie auto

        KP->>K: connexion + envoi
        K-->>KP: ack
        KP-->>APP: callback / future
    end
```

</div>

---

#### Sélection de la partition
C'est la **bibliothèque producer** qui détermine dans quelle partition écrire chaque message. Elle le fait par **hachage de la clé** (si une clé est fournie) ou par **round-robin** (si aucune clé n'est définie). Le développeur peut aussi forcer une partition spécifique via le `ProducerRecord`.

##### Schéma — 3 stratégies de routage vers la partition

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        REC["📦 ProducerRecord"]
        REC --> Q{"❓ Stratégie ?"}
        Q -->|"key fournie"| HASH["#️⃣ hash(key) % N"]
        Q -->|"key = null"| RR["🔄 Round-robin"]
        Q -->|"partition forcée<br/>dans le record"| FORCE["🎯 Partition explicite"]

        HASH --> P1["📦 Partition cible"]
        RR --> P1
        FORCE --> P1
    end

    style REC fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style Q fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:2px
    style HASH fill:#7b1fa2,color:#fff
    style RR fill:#039be5,color:#fff
    style FORCE fill:#e53935,color:#fff
    style P1 fill:#fff8e1,stroke:#f57c00,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

- Une application de **thermostat connecté** qui publie régulièrement des relevés de température dans un topic Kafka.
- Tout service applicatif qui doit **injecter des événements** dans un pipeline de données en temps réel.

##### Schéma — Exemple : thermostat connecté → Kafka

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        T["🌡️ Thermostat<br/>(application producer)"]
        T -->|"new ProducerRecord<br/>(thermostat-readings,<br/>sensor-42, 24°C)"| KP["☕ KafkaProducer"]
        KP -->|"send()"| K[("⚡ Kafka")]
        K --> TOPIC["📋 topic:<br/>thermostat-readings"]
        TOPIC --> PIPE["🔧 Pipeline<br/>temps réel"]
    end

    style T fill:#fb8c00,color:#fff
    style KP fill:#43a047,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style TOPIC fill:#fff8e1,stroke:#f57c00
    style PIPE fill:#7b1fa2,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Comparaisons

| Langue / Plateforme          | Support                                      |
|------------------------------|----------------------------------------------|
| Java                         | Natif (officiel, fonctionnalités en premier) |
| Python, Go, JavaScript, .NET | Support officiel Confluent                   |
| Autres langages              | Drivers communautaires disponibles           |

##### Schéma — Support multi-langage

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        K[("⚡ Kafka")]
        K --> J["☕ Java<br/>🟢 Natif officiel<br/>(features en premier)"]
        K --> CONF["🔵 Python / Go / JS / .NET<br/>🟢 Officiel Confluent"]
        K --> COM["🟠 Autres langages<br/>🟡 Communautaire"]
    end

    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style J fill:#43a047,color:#fff
    style CONF fill:#039be5,color:#fff
    style COM fill:#fb8c00,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Mises en garde

- **Sérialisation simpliste** : convertir un objet en JSON sous forme de `String` est fonctionnel mais peu robuste. Il vaut mieux utiliser un sérialiseur adapté au schéma réel de l'objet, notamment via le Schema Registry.
- **Ne pas lister tous les brokers** dans `bootstrap.servers` : quelques adresses suffisent pour établir la connexion initiale et récupérer les métadonnées complètes.
- **L'API semble simple, mais ne l'est pas** : le `KafkaProducer` effectue de nombreuses opérations complexes en arrière-plan (gestion des erreurs réseau, idempotence, batching). Il est important de comprendre ces mécanismes pour configurer correctement le producer selon les besoins (latence faible vs débit élevé).

##### Schéma — Pièges à éviter

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        WARN["⚠️ Pièges courants"]
        WARN --> W1["❌ Sérialiser via JSON.toString()<br/><i>peu robuste, pas de schéma</i>"]
        WARN --> W2["❌ Lister 50 brokers<br/>dans bootstrap.servers<br/><i>2-3 suffisent</i>"]
        WARN --> W3["❌ Croire l'API triviale<br/><i>acks, retries, idempotence,<br/>batching à comprendre</i>"]
    end

    style WARN fill:#ff5722,color:#fff,stroke:#bf360c,stroke-width:3px
    style W1 fill:#ffcdd2,color:#000,stroke:#c62828
    style W2 fill:#ffcdd2,color:#000,stroke:#c62828
    style W3 fill:#ffcdd2,color:#000,stroke:#c62828
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Points à retenir

- Un **producer** est toute application cliente qui écrit dans Kafka.
- L'API principale repose sur deux classes : `KafkaProducer` et `ProducerRecord`.
- La configuration se fait via des paramètres clé-valeur ; `bootstrap.servers` et `acks` sont les plus importants à connaître.
- La clé et la valeur d'un message sont des **bytes** : la sérialisation est à la charge du producer.
- C'est le producer qui décide de la **partition cible**, par hachage de la clé ou round-robin.
- La complexité réelle est masquée par la simplicité de l'API : acknowledgements, retransmissions, idempotence et batching sont gérés automatiquement.

##### Schéma de synthèse — Carte mentale du module Producers

<div align="center">

```mermaid
mindmap
  root((🏭 Producers<br/>Kafka))
    Définition
      Application cliente
      Écrit dans Kafka
      Pas un broker
    API Java
      KafkaProducer
        Façade simple
        Plomberie complexe
      ProducerRecord
        topic obligatoire
        key valeur
        timestamp partition headers
    Configuration
      Clé-valeur
      bootstrap.servers
      acks
      5 à 10 params en réel
    Bootstrap
      2-3 brokers suffisent
      Découverte métadonnées
      Communication tous brokers
    Sérialisation
      Bytes
      Natifs primitifs
      Avro Protobuf
      Schema Registry
    Routage partition
      hash key
      round-robin
      partition forcée
    Multi-langage
      Java natif
      Python Go JS .NET
      Communautaire
    Pièges
      Sérialisation simpliste
      Trop de bootstrap servers
      API faussement simple
```

</div>
