# Apache Kafka — Introduction aux Brokers

## Table des matières
- [Module 1 : Les Brokers Kafka](#module-1--les-brokers-kafka)

---

## Module 1 : Les Brokers Kafka

### Sujet

Ce module introduit les brokers Apache Kafka : ce qu'ils sont physiquement, leur rôle dans l'architecture Kafka, et leur évolution récente avec la suppression de ZooKeeper.

---

### Concepts clés

**Broker**
Un broker est une machine (serveur physique, instance cloud, Raspberry Pi, etc.) qui exécute le processus serveur Kafka (une JVM). C'est l'unité de base de l'infrastructure Kafka. Chaque broker héberge des partitions et traite les requêtes de lecture et d'écriture des clients.

##### Schéma — Anatomie d'un broker

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph BROKER ["🖥️ Broker Kafka"]
            direction TB
            HW["💻 Machine<br/>(serveur, VM, cloud, Raspberry Pi...)"]
            HW --> JVM["☕ JVM<br/>(processus serveur Kafka)"]
            JVM --> P["📦 Partitions hébergées"]
            JVM --> REQ["📡 Traitement requêtes<br/>(lecture / écriture)"]
            JVM --> SSD["💾 Stockage local<br/>(SSD)"]
        end
    end

    style BROKER fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style HW fill:#90caf9,color:#000
    style JVM fill:#fb8c00,color:#fff
    style P fill:#fff8e1,stroke:#f57c00
    style REQ fill:#c8e6c9,stroke:#2e7d32
    style SSD fill:#9e9e9e,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Cluster Kafka**
Un ensemble de brokers interconnectés forme un cluster Kafka. Un cluster typique contient plusieurs brokers (ex. : trois), ce qui permet la distribution de la charge et la réplication des données.

##### Schéma — Cluster Kafka : plusieurs brokers interconnectés

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph CLUSTER ["🌐 Cluster Kafka"]
            direction LR
            B1[("🖥️ Broker 1")]
            B2[("🖥️ Broker 2")]
            B3[("🖥️ Broker 3")]
            B1 <-->|"réplication<br/>coordination"| B2
            B2 <-->|"réplication<br/>coordination"| B3
            B1 <-->|"réplication<br/>coordination"| B3
        end
        CLUSTER -.->|"objectifs"| GOALS["⚖️ Distribution de charge<br/>🛡️ Réplication / tolérance aux pannes"]
    end

    style CLUSTER fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style B1 fill:#fff8e1,stroke:#f57c00
    style B2 fill:#fff8e1,stroke:#f57c00
    style B3 fill:#fff8e1,stroke:#f57c00
    style GOALS fill:#c8e6c9,stroke:#1b5e20
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Stockage local (local storage)**
Historiquement, les brokers sont étroitement couplés à un stockage local — généralement des SSDs situés physiquement à proximité du processeur. Ce couplage fort est une caractéristique importante d'Apache Kafka open-source.

##### Schéma — Couplage broker / stockage local

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph BOX ["🖥️ Machine physique"]
            direction LR
            CPU["🧠 CPU<br/>(processus Kafka)"] <-->|"I/O rapide<br/>(proximité)"| SSD["💾 SSD local"]
        end
        BOX -.->|"caractéristique<br/>Apache Kafka OSS"| NOTE["⚠️ Couplage fort<br/>broker ⇄ disque"]
    end

    style BOX fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style CPU fill:#fb8c00,color:#fff
    style SSD fill:#9e9e9e,color:#fff
    style NOTE fill:#fff3e0,stroke:#e65100
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**KRaft (Kafka Raft)**
Protocole interne à Kafka, basé sur l'algorithme Raft, qui permet aux brokers de maintenir eux-mêmes une vue cohérente des métadonnées du cluster. Il remplace ZooKeeper depuis Apache Kafka 4.0.

##### Schéma — Avant ZooKeeper vs Après KRaft

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph BEFORE ["🟠 Avant Kafka 4.0 (avec ZooKeeper)"]
            direction TB
            ZK[("🦓 ZooKeeper<br/>(externe)")]
            BB1["🖥️ Broker 1"] --> ZK
            BB2["🖥️ Broker 2"] --> ZK
            BB3["🖥️ Broker 3"] --> ZK
            ZK -.-> ZKINFO["⚠️ Composant externe<br/>à opérer en plus"]
        end

        subgraph AFTER ["🟢 Depuis Kafka 4.0 (KRaft)"]
            direction TB
            AB1["🖥️ Broker 1<br/>(KRaft)"]
            AB2["🖥️ Broker 2<br/>(KRaft)"]
            AB3["🖥️ Broker 3<br/>(KRaft)"]
            AB1 <-->|"Raft"| AB2
            AB2 <-->|"Raft"| AB3
            AB1 <-->|"Raft"| AB3
            AB1 -.-> AINFO["✅ Métadonnées gérées<br/>en interne"]
        end

        BEFORE -.->|"évolution"| AFTER
    end

    style BEFORE fill:#fff3e0,stroke:#e65100
    style AFTER fill:#e8f5e9,stroke:#2e7d32
    style ZK fill:#9e9e9e,color:#fff
    style ZKINFO fill:#ffe0b2,color:#000
    style AINFO fill:#c8e6c9,color:#000
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

**Démarrage d'un broker**
Il suffit de télécharger le tarball Apache Kafka, de le décompresser, et d'exécuter le script fourni pour lancer le processus JVM. Il est également possible d'utiliser une image Docker standard et de composer un groupe de brokers via Docker Compose — chaque conteneur constituant alors un broker distinct.

##### Schéma — Deux façons de démarrer des brokers

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph WAY1 ["📦 Voie 1 : Tarball"]
            direction TB
            T1["⬇️ Télécharger<br/>tarball Kafka"] --> T2["📂 Décompresser"]
            T2 --> T3["▶️ Script de démarrage"]
            T3 --> T4["☕ Process JVM<br/>= 1 broker"]
        end

        subgraph WAY2 ["🐳 Voie 2 : Docker Compose"]
            direction TB
            D1["📄 docker-compose.yml"] --> D2["🐳 docker compose up"]
            D2 --> D3["📦 N conteneurs<br/>= N brokers"]
        end
    end

    style WAY1 fill:#e3f2fd,stroke:#1565c0
    style WAY2 fill:#f3e5f5,stroke:#6a1b9a
    style T4 fill:#fff8e1,stroke:#f57c00
    style D3 fill:#fff8e1,stroke:#f57c00
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Distribution des partitions sur les brokers**
Chaque broker héberge une ou plusieurs partitions. Lorsqu'un topic possède trois partitions et que le cluster contient trois brokers, chaque broker prend en charge une partition. Si un topic n'a que deux partitions, seuls deux brokers hébergent des partitions pour ce topic. Cette distribution est le mécanisme central de la scalabilité de Kafka.

##### Schéma — Distribution des partitions selon leur nombre

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph CASE1 ["📊 Topic A : 3 partitions / 3 brokers"]
            direction LR
            CA1["🖥️ Broker 1<br/>📦 P0"]
            CA2["🖥️ Broker 2<br/>📦 P1"]
            CA3["🖥️ Broker 3<br/>📦 P2"]
            CA1 -.->|"✅ équilibré"| OK1["3 brokers utilisés"]
        end

        subgraph CASE2 ["📊 Topic B : 2 partitions / 3 brokers"]
            direction LR
            CB1["🖥️ Broker 1<br/>📦 P0"]
            CB2["🖥️ Broker 2<br/>📦 P1"]
            CB3["🖥️ Broker 3<br/>(aucune partition<br/>pour ce topic)"]
            CB1 -.->|"⚠️ partiel"| OK2["2 brokers utilisés"]
        end
    end

    style CASE1 fill:#e8f5e9,stroke:#2e7d32
    style CASE2 fill:#fff3e0,stroke:#e65100
    style CA1 fill:#fff8e1,stroke:#f57c00
    style CA2 fill:#fff8e1,stroke:#f57c00
    style CA3 fill:#fff8e1,stroke:#f57c00
    style CB1 fill:#fff8e1,stroke:#f57c00
    style CB2 fill:#fff8e1,stroke:#f57c00
    style CB3 fill:#eceff1,stroke:#9e9e9e,stroke-dasharray:3 3
    style OK1 fill:#a5d6a7
    style OK2 fill:#ffe082
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Traitement des requêtes**
Les brokers reçoivent et traitent les requêtes entrantes des applications clientes : écriture de nouveaux messages dans les partitions et lecture de messages depuis les partitions. Ils effectuent les opérations d'I/O réelles sur les logs de partitions.

##### Schéma — Flux de requêtes producteur / consommateur

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant P as 🏭 Producteur
    participant B as 🖥️ Broker
    participant L as 📦 Partition (log)
    participant C as 📱 Consommateur

    rect rgb(250, 250, 250)
        P->>B: ✍️ Produce(message)
        B->>L: I/O écriture
        L-->>B: offset attribué
        B-->>P: ack

        Note over B,L: ⏱️ ...

        C->>B: 📖 Fetch(offset)
        B->>L: I/O lecture
        L-->>B: messages
        B-->>C: batch de messages
    end
```

</div>

---

**Gestion de la réplication**
Les brokers sont également responsables de la réplication des données entre eux, ce qui assure la tolérance aux pannes (sujet abordé dans le module suivant).

##### Schéma — Réplication entre brokers

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur"] --> B1["🖥️ Broker 1<br/>📦 P0 (leader)"]
        B1 -->|"réplique"| B2["🖥️ Broker 2<br/>📦 P0 (replica)"]
        B1 -->|"réplique"| B3["🖥️ Broker 3<br/>📦 P0 (replica)"]
        B1 -.->|"objectif"| GOAL["🛡️ Tolérance aux pannes"]
    end

    style P fill:#43a047,color:#fff
    style B1 fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style B2 fill:#fff8e1,stroke:#f57c00
    style B3 fill:#fff8e1,stroke:#f57c00
    style GOAL fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Gestion des métadonnées avec KRaft**
Depuis Apache Kafka 4.0, les brokers assurent eux-mêmes la cohérence des métadonnées du cluster grâce à leur propre implémentation du protocole Raft, appelée KRaft. Cette fonctionnalité était auparavant assurée par Apache ZooKeeper.

##### Schéma — KRaft : consensus interne sur les métadonnées

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph QUORUM ["🗳️ Quorum KRaft"]
            direction LR
            B1["🖥️ Broker 1<br/>(controller candidate)"]
            B2["🖥️ Broker 2<br/>(controller leader)"]
            B3["🖥️ Broker 3<br/>(controller candidate)"]
            B1 <-->|"vote / log Raft"| B2
            B2 <-->|"vote / log Raft"| B3
            B1 <-->|"vote / log Raft"| B3
        end
        QUORUM -.->|"maintient"| META["📋 Métadonnées du cluster<br/>(topics, partitions, leaders...)"]
        META -.->|"vue cohérente"| ALL["✅ Tous les brokers"]
    end

    style QUORUM fill:#e3f2fd,stroke:#0d47a1,stroke-width:2px
    style B1 fill:#fff8e1,stroke:#f57c00
    style B2 fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style B3 fill:#fff8e1,stroke:#f57c00
    style META fill:#f3e5f5,stroke:#6a1b9a
    style ALL fill:#c8e6c9,stroke:#2e7d32
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

Les brokers sont particulièrement visibles et importants dans les contextes suivants :
- Lancement en local pour le développement ou les tests.
- Déploiement autogéré d'Apache Kafka (on-premise ou cloud self-managed).
- Apprentissage des fondamentaux de Kafka, même si l'on prévoit d'utiliser ensuite un service managé.

Dans les services cloud managés comme Confluent Cloud, les brokers sont entièrement abstraits : l'utilisateur interagit uniquement avec les topics, messages et connecteurs. La notion de broker disparaît de l'interface.

##### Schéma — Visibilité des brokers selon le contexte

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph SELF ["🛠️ Self-managed (Apache Kafka)"]
            direction TB
            U1["👤 Utilisateur"] --> SBR["🖥️🖥️🖥️ Brokers visibles"]
            SBR --> ST["📋 Topics & messages"]
        end

        subgraph CLOUD ["☁️ Service managé (Confluent Cloud)"]
            direction TB
            U2["👤 Utilisateur"] --> CT["📋 Topics & messages"]
            HIDE["🖥️🖥️🖥️ Brokers cachés<br/>(abstraction)"] -.-> CT
        end
    end

    style SELF fill:#fff3e0,stroke:#e65100
    style CLOUD fill:#e3f2fd,stroke:#0d47a1
    style SBR fill:#fff8e1,stroke:#f57c00
    style HIDE fill:#eceff1,stroke:#9e9e9e,stroke-dasharray:3 3
    style ST fill:#c8e6c9,stroke:#2e7d32
    style CT fill:#c8e6c9,stroke:#2e7d32
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Comparaisons

| Aspect                    | Apache Kafka (self-managed)          | Confluent Cloud (managé)        |
|---------------------------|--------------------------------------|---------------------------------|
| Visibilité des brokers    | Totale — à configurer et administrer | Aucune — entièrement abstraits  |
| Stockage                  | Local (SSD couplé)                   | Géré par le fournisseur         |
| Gestion des métadonnées   | KRaft (depuis v4.0)                  | Transparente pour l'utilisateur |
| Complexité opérationnelle | Élevée                               | Faible                          |

##### Schéma — Comparaison synthétique self-managed vs managé

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph SM ["🛠️ Apache Kafka (self-managed)"]
            direction TB
            SM1["👁️ Brokers : visibles"]
            SM2["💾 Stockage : SSD local couplé"]
            SM3["🗳️ Métadonnées : KRaft"]
            SM4["⚙️ Complexité : élevée"]
        end

        subgraph CM ["☁️ Confluent Cloud (managé)"]
            direction TB
            CM1["🙈 Brokers : abstraits"]
            CM2["💾 Stockage : géré par le fournisseur"]
            CM3["🗳️ Métadonnées : transparent"]
            CM4["⚙️ Complexité : faible"]
        end
    end

    style SM fill:#fff3e0,stroke:#e65100
    style CM fill:#e3f2fd,stroke:#0d47a1
    style SM4 fill:#ffcdd2
    style CM4 fill:#c8e6c9
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Mises en garde

- **ZooKeeper est obsolète** : depuis Apache Kafka 4.0, ZooKeeper n'est plus inclus ni supporté. Toute documentation ou ressource faisant référence à ZooKeeper est donc dépassée. Il ne faut plus en tenir compte pour les nouvelles installations.
- **Connaissance des brokers utile même sans les opérer** : même si l'on utilise uniquement un service managé, comprendre les abstractions de base (brokers, partitions, réplication) reste nécessaire pour bien utiliser Kafka.

##### Schéma — Pièges à éviter

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        WARN["⚠️ Mises en garde"]
        WARN --> W1["❌ Suivre une doc ZooKeeper<br/><i>obsolète depuis Kafka 4.0</i>"]
        WARN --> W2["❌ Ignorer les brokers<br/>parce que c'est managé<br/><i>les concepts restent essentiels</i>"]
    end

    style WARN fill:#ff5722,color:#fff,stroke:#bf360c,stroke-width:3px
    style W1 fill:#ffcdd2,color:#000,stroke:#c62828
    style W2 fill:#ffcdd2,color:#000,stroke:#c62828
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Points à retenir

- Un **broker** = une machine exécutant le processus serveur Kafka, avec stockage local.
- Plusieurs brokers forment un **cluster Kafka**.
- Les brokers hébergent des **partitions**, traitent les requêtes clients (lecture/écriture) et gèrent la **réplication**.
- Le nombre de partitions par topic est configurable indépendamment selon les besoins de scalabilité.
- Depuis **Apache Kafka 4.0**, ZooKeeper est supprimé : les brokers utilisent **KRaft** pour la gestion des métadonnées.
- Dans les services cloud managés (ex. Confluent Cloud), les brokers sont invisibles pour l'utilisateur final.

##### Schéma de synthèse — Carte mentale du module Brokers

<div align="center">

```mermaid
mindmap
  root((🖥️ Brokers<br/>Kafka))
    Définition
      Machine
      Processus JVM
      Stockage local SSD
    Cluster
      Plusieurs brokers
      Distribution de charge
      Réplication
    Rôles
      Héberger partitions
      Traiter requêtes
        Écriture producteur
        Lecture consommateur
      Répliquer données
      Gérer métadonnées
    Démarrage
      Tarball + script
      Docker Compose
    KRaft
      Depuis Kafka 4.0
      Remplace ZooKeeper
      Consensus interne
    Contextes
      Self-managed
        Brokers visibles
      Confluent Cloud
        Brokers abstraits
    Pièges
      Doc ZooKeeper obsolète
      Comprendre quand même
```

</div>
