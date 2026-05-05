# Apache Kafka — Consumers

## Table des matières
- [Module 1 : Introduction aux Consumers Kafka](#module-1--introduction-aux-consumers-kafka)
- [Module 2 : Consumer Offset Tracking](#module-2--consumer-offset-tracking)
- [Module 3 : Consumer Groups et scalabilité](#module-3--consumer-groups-et-scalabilité)

---

## Module 1 : Introduction aux Consumers Kafka

### Sujet
Les consumers sont les composants clients situés **en dehors du cluster Kafka**, responsables de la **lecture des messages** depuis les topics Kafka. Ce module présente l'API consumer en Java et explique le fonctionnement de base de la consommation de messages.

##### Schéma — Le consumer dans l'écosystème

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        K[("⚡ Cluster Kafka<br/>(brokers + topics)")]
        K --> C1["📱 Consumer A<br/>(application externe)"]
        K --> C2["📱 Consumer B<br/>(application externe)"]
        C1 -.->|"lit en continu"| APP1["💻 Logique métier"]
        C2 -.->|"lit en continu"| APP2["💻 Logique métier"]
    end

    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style C1 fill:#fb8c00,color:#fff
    style C2 fill:#fb8c00,color:#fff
    style APP1 fill:#43a047,color:#fff
    style APP2 fill:#43a047,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Concepts clés

- **KafkaConsumer** : classe Java utilisée pour se connecter au cluster Kafka et lire des messages. Elle fonctionne sur le même principe que le `KafkaProducer` : on lui fournit une `Map` de paires clé-valeur (fichier de configuration).
- **bootstrap.servers** : configuration minimale obligatoire. Il n'est pas nécessaire de lister tous les brokers — un seul broker accessible suffit pour que le consumer découvre automatiquement la topologie complète du cluster.
- **subscribe()** : méthode appelée sur le `KafkaConsumer` pour s'abonner à un ou plusieurs topics. Elle prend **obligatoirement une liste** (même pour un seul topic). Elle supporte également une **expression régulière** (RegEx) pour s'abonner à tous les topics dont le nom correspond au pattern.
- **poll()** : méthode appelée en boucle infinie pour interroger le cluster et récupérer les nouveaux messages disponibles. Sous le capot, la librairie identifie les partitions des topics souscrits, localise les brokers leaders de ces partitions, et leur envoie des requêtes réseau.
- **ConsumerRecords<K, V>** : collection générique retournée par `poll()`, contenant les messages reçus. Les types génériques (`String, String` dans l'exemple) doivent correspondre aux types utilisés lors de la production.

##### Schéma — Anatomie de l'API KafkaConsumer

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        APP["💻 Application Java"] --> KC["☕ KafkaConsumer<K, V>"]
        KC --> CFG["⚙️ Map de config<br/>(bootstrap.servers, ...)"]
        KC --> SUB["📋 subscribe(List<String>)<br/>ou RegEx"]
        KC --> POLL["🔁 poll(Duration)"]
        POLL --> CR["📦 ConsumerRecords<K, V>"]
        CR --> REC["🧱 ConsumerRecord<br/>key / value / partition<br/>timestamp / headers"]
    end

    style APP fill:#43a047,color:#fff
    style KC fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:2px
    style CFG fill:#fff8e1,stroke:#f57c00
    style SUB fill:#e3f2fd,stroke:#1565c0
    style POLL fill:#7b1fa2,color:#fff,stroke:#4a148c,stroke-width:2px
    style CR fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style REC fill:#bbdefb,stroke:#1565c0
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Fonctionnement

1. Instancier un `KafkaConsumer` avec les propriétés de connexion.
2. Appeler `subscribe(List<String> topics)` pour indiquer quel(s) topic(s) lire.
3. Entrer dans une **boucle infinie** et appeler `poll(Duration)` à chaque itération.
4. Itérer sur la collection `ConsumerRecords` reçue et traiter chaque `ConsumerRecord`.
5. Chaque enregistrement expose notamment : **key**, **value**, partition d'origine, timestamp, headers.

##### Schéma — La poll loop : cycle de vie d'un consumer

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant APP as 💻 App
    participant KC as ☕ KafkaConsumer
    participant K as ⚡ Cluster Kafka

    rect rgb(250, 250, 250)
        APP->>KC: 1️⃣ new KafkaConsumer(config)
        APP->>KC: 2️⃣ subscribe([topic])

        loop ♾️ Boucle infinie
            APP->>KC: 3️⃣ poll(Duration)
            KC->>K: requêtes vers brokers leaders
            K-->>KC: messages
            KC-->>APP: ConsumerRecords<K,V>
            APP->>APP: 4️⃣ traite chaque record<br/>(key, value, partition...)
        end
    end
```

</div>

### Cas d'usage
- Applications de streaming où les messages arrivent en continu et doivent être traités en temps réel.
- La boucle infinie est le paradigme normal : contrairement au traitement par lots, le streaming n'a pas de "dernier message". Il y a toujours potentiellement de nouveaux messages à lire.

##### Schéma — Streaming continu vs traitement par lots

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph BATCH ["📦 Traitement par lots"]
            direction TB
            B1["📂 Fichier figé"] --> B2["▶️ Job"] --> B3["✅ Fin"]
            B3 -.->|"a un dernier élément"| BEND["🛑 Termine"]
        end

        subgraph STREAM ["♾️ Streaming Kafka"]
            direction TB
            S1["📨 Flux continu"] --> S2["🔁 poll()"] --> S2
            S2 -.->|"jamais de dernier message"| SEND["⏳ Tourne en permanence"]
        end
    end

    style BATCH fill:#fff3e0,stroke:#e65100
    style STREAM fill:#e8f5e9,stroke:#2e7d32
    style BEND fill:#ffe0b2
    style SEND fill:#a5d6a7
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Comparaisons

| Aspect                    | Kafka Consumer                        | Queue traditionnelle                           |
|---------------------------|---------------------------------------|------------------------------------------------|
| Persistance après lecture | Le message **reste** dans le topic    | Le message est **supprimé** après consommation |
| Nombre de lecteurs        | **Illimité** (consumers indépendants) | Généralement un seul consommateur par message  |

##### Schéma — Consumer Kafka vs Queue traditionnelle

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph KAFKA ["📜 Kafka (log persistant)"]
            direction TB
            KM["📦 Message"] --> KR["🔄 Reste dans le topic"]
            KR --> KCN["📱 Consumers ∞ indépendants"]
        end

        subgraph QUEUE ["📤 Queue traditionnelle"]
            direction TB
            QM["📦 Message"] --> QR["🗑️ Supprimé après lecture"]
            QR --> QCN["📱 1 seul consommateur<br/>par message"]
        end
    end

    style KAFKA fill:#e8f5e9,stroke:#2e7d32
    style QUEUE fill:#ffebee,stroke:#c62828
    style KR fill:#a5d6a7
    style QR fill:#ef9a9a
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Mises en garde
- Les types génériques du `KafkaConsumer<K, V>` doivent **correspondre** à ceux utilisés par le producer. Une incohérence de types entraîne des erreurs de désérialisation.
- La valeur d'un message est souvent un **objet métier** (domain object) avec un schéma structuré — cela sera couvert plus en détail dans les modules sur les schémas.

### Points à retenir
- Kafka est un **log**, pas une queue : consommer un message ne le supprime pas.
- Plusieurs consumers **indépendants** peuvent lire les mêmes messages simultanément.
- La méthode `poll()` est le cœur de tout consumer Kafka.

---

## Module 2 : Consumer Offset Tracking

### Sujet
Ce module explique comment Kafka gère la **position de lecture** de chaque consumer via le mécanisme des **offsets**, permettant à un consumer de reprendre là où il s'était arrêté en cas de panne ou de redémarrage.

##### Schéma — Position de lecture dans une partition

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph PART ["📦 Partition"]
            direction LR
            O0["[0]"] --> O1["[1]"] --> O2["[2]"] --> O3["[3]"] --> O4["[4]"] --> O5["[5]"] --> ONEXT["[6] →"]
        end

        C["📱 Consumer"] -.->|"committed offset = 3"| O3
        O3 -.->|"reprendra à"| O4
    end

    style PART fill:#e3f2fd,stroke:#0d47a1
    style O0 fill:#bbdefb
    style O1 fill:#bbdefb
    style O2 fill:#bbdefb
    style O3 fill:#43a047,color:#fff,stroke:#1b5e20,stroke-width:3px
    style O4 fill:#fff9c4,stroke:#f57f17
    style O5 fill:#fff8e1
    style ONEXT fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:3 3
    style C fill:#fb8c00,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Concepts clés

- **Offset** : identifiant numérique unique attribué à chaque message dans une partition. Il représente la position du message dans le log de la partition.
- **Consumer Offset Commit** : action par laquelle un consumer **signale au cluster** l'offset du dernier message qu'il a correctement traité. Cela permet au cluster de mémoriser la progression du consumer.
- **Internal Topic (`__consumer_offsets`)** : topic interne spécial dans lequel Kafka stocke les offsets commités par les consumers. Kafka utilise ainsi ses propres mécanismes pour implémenter cette fonctionnalité.

##### Schéma — Stockage des offsets dans `__consumer_offsets`

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        C["📱 Consumer"] -->|"commit(offset = 42)"| K[("⚡ Kafka")]
        K --> CO[("📋 __consumer_offsets<br/>(topic interne)")]
        CO -.->|"ré-utilise les<br/>mêmes mécanismes<br/>que les autres topics"| INFO["♻️ Pas de dépendance externe"]
    end

    style C fill:#fb8c00,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style CO fill:#7b1fa2,color:#fff,stroke:#4a148c,stroke-width:3px
    style INFO fill:#e1bee7,stroke:#6a1b9a
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Fonctionnement

1. Le consumer lit les messages (offset 1, 2, 3…).
2. Après traitement réussi d'un message, il **commite l'offset** correspondant vers le cluster.
3. Si le traitement d'un message échoue (ex. : offset 3), l'offset n'est **pas commité**.
4. Lors du redémarrage du consumer, il reprend à partir du dernier offset commité, et retente le traitement du message raté.

**Optimisation des commits** : les commits ne se font pas nécessairement message par message (ce serait coûteux en performances). Ils sont généralement **regroupés par batch**, déclenchés selon :
- Un seuil de nombre de messages traités, ou
- Un intervalle de temps configuré.

Ces paramètres sont documentés et configurables selon les besoins de performance.

##### Schéma — Échec de traitement et reprise au redémarrage

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant C as 📱 Consumer
    participant K as ⚡ Kafka

    rect rgb(250, 250, 250)
        Note over C: 🟢 Marche normale
        K-->>C: msg offset 1
        C->>K: commit(1)
        K-->>C: msg offset 2
        C->>K: commit(2)

        Note over C: 🔴 Échec de traitement
        K-->>C: msg offset 3
        C--xC: ❌ erreur — pas de commit

        Note over C: 💥 Crash / redémarrage
        C->>K: reprise au<br/>dernier offset commité (2)
        K-->>C: msg offset 3 (rejoué)
        C->>K: commit(3)
    end
```

</div>

##### Schéma — Commits batchés par seuil ou par temps

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        TRG{"❓ Déclencheur de commit"}
        TRG -->|"N messages traités"| T1["🔢 Seuil de nombre"]
        TRG -->|"Δt écoulé"| T2["⏱️ Intervalle de temps"]

        T1 --> CMT["✅ commit groupé"]
        T2 --> CMT

        CMT -.->|"trop fréquent"| BAD1["🐌 overhead réseau"]
        CMT -.->|"trop rare"| BAD2["🔁 retraitement important<br/>en cas de panne"]
    end

    style TRG fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:3px
    style T1 fill:#e3f2fd,stroke:#1565c0
    style T2 fill:#e3f2fd,stroke:#1565c0
    style CMT fill:#43a047,color:#fff
    style BAD1 fill:#ffcdd2,color:#000
    style BAD2 fill:#ffcdd2,color:#000
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Mises en garde
- Si un consumer traite un message mais **ne commite pas** l'offset (ex. : crash avant le commit), il retraitera ce message au prochain démarrage. Il faut donc concevoir les consumers pour être **idempotents** si possible.
- La granularité des commits est un paramètre d'optimisation : trop fréquent = overhead réseau ; pas assez fréquent = risque de retraitement important en cas de panne.

### Points à retenir
- Les offsets permettent à Kafka de savoir **exactement où** chaque consumer en est dans sa lecture.
- Le commit d'offset est une **confirmation de traitement réussi**, pas simplement une confirmation de réception.
- Kafka stocke ces offsets dans son propre topic interne — pas de dépendance externe nécessaire.

---

## Module 3 : Consumer Groups et scalabilité

### Sujet
Ce module introduit le concept de **consumer groups**, qui permet à plusieurs consumers de collaborer pour lire un même topic de manière scalable et parallèle.

##### Schéma — Consumer Group : coopération par répartition des partitions

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph TOPIC ["📋 Topic (3 partitions)"]
            direction TB
            P0["📦 P0"]
            P1["📦 P1"]
            P2["📦 P2"]
        end

        subgraph GROUP ["👥 Consumer Group"]
            direction TB
            G1["📱 Consumer 1"]
            G2["📱 Consumer 2"]
            G3["📱 Consumer 3"]
        end

        P0 --> G1
        P1 --> G2
        P2 --> G3

        GROUP -.->|"⚖️ partitions<br/>réparties entre membres"| OK["✅ Parallélisme"]
    end

    style TOPIC fill:#e3f2fd,stroke:#1565c0
    style GROUP fill:#e8f5e9,stroke:#2e7d32
    style OK fill:#c8e6c9,stroke:#1b5e20,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Concepts clés

- **Consumer Group** : ensemble de consumers qui coopèrent pour lire un topic. Chaque consumer du groupe est **mono-threadé** : il ne traite qu'un message à la fois, issu d'une seule partition à la fois.
- **Consumer indépendant** : un consumer peut aussi opérer de manière totalement indépendante d'un autre consumer, chacun lisant l'intégralité du topic à son propre rythme (ex. : Consumer A et Consumer B lisent les mêmes messages indépendamment).

##### Schéma — Consumer Group vs Consumers indépendants

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph IND ["🔀 Consumers indépendants"]
            direction TB
            TIND["📋 Topic"]
            TIND --> CA["📱 Consumer A<br/>(lit tout)"]
            TIND --> CB["📱 Consumer B<br/>(lit tout)"]
            CA -.-> CAINFO["chacun son offset<br/>même messages"]
            CB -.-> CAINFO
        end

        subgraph CG ["👥 Consumer Group"]
            direction TB
            TCG["📋 Topic (3 partitions)"]
            TCG --> CG1["📱 C1"]
            TCG --> CG2["📱 C2"]
            TCG --> CG3["📱 C3"]
            CG1 -.-> CGINFO["partitions partagées<br/>parallélisme"]
            CG2 -.-> CGINFO
            CG3 -.-> CGINFO
        end
    end

    style IND fill:#fff3e0,stroke:#e65100
    style CG fill:#e8f5e9,stroke:#2e7d32
    style CAINFO fill:#ffe0b2
    style CGINFO fill:#a5d6a7
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Fonctionnement

- Un topic à 3 partitions peut être lu par un seul consumer (Consumer A), qui reçoit alors des messages provenant de **n'importe laquelle** des 3 partitions.
- Ajouter un Consumer B **n'interfère pas** avec Consumer A : ils lisent de façon indépendante, chacun gérant son propre offset.
- Les consumers d'un même **consumer group** se **répartissent les partitions** entre eux, permettant un traitement parallèle.

##### Schéma — 3 scénarios de consommation

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph S1 ["1️⃣ 1 consumer / 3 partitions"]
            direction LR
            S1P0["📦 P0"] --> S1C["📱 Consumer A"]
            S1P1["📦 P1"] --> S1C
            S1P2["📦 P2"] --> S1C
        end

        subgraph S2 ["2️⃣ 2 consumers indépendants"]
            direction LR
            S2T["📋 Topic"]
            S2T --> S2A["📱 Consumer A<br/>(lit tout)"]
            S2T --> S2B["📱 Consumer B<br/>(lit tout)"]
        end

        subgraph S3 ["3️⃣ Consumer group (3 membres)"]
            direction LR
            S3P0["📦 P0"] --> S3C1["📱 C1"]
            S3P1["📦 P1"] --> S3C2["📱 C2"]
            S3P2["📦 P2"] --> S3C3["📱 C3"]
        end
    end

    style S1 fill:#e3f2fd,stroke:#1565c0
    style S2 fill:#fff3e0,stroke:#e65100
    style S3 fill:#e8f5e9,stroke:#2e7d32
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Comparaisons

| Configuration                        | Comportement                                                             |
|--------------------------------------|--------------------------------------------------------------------------|
| 1 consumer, 3 partitions             | Le consumer lit depuis les 3 partitions séquentiellement                 |
| 2 consumers indépendants             | Chacun lit toutes les partitions indépendamment (duplication de lecture) |
| Consumer group (plusieurs consumers) | Les partitions sont réparties entre les membres du groupe (parallélisme) |

### Mises en garde
- Les consumers sont **mono-threadés** par nature. Pour augmenter le débit de traitement, il faut augmenter le nombre de consumers (et idéalement le nombre de partitions du topic).

##### Schéma — Limite : un consumer par partition au maximum dans un groupe

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph OK ["✅ 3 consumers / 3 partitions"]
            direction LR
            OP0["📦 P0"] --> OC1["📱 C1"]
            OP1["📦 P1"] --> OC2["📱 C2"]
            OP2["📦 P2"] --> OC3["📱 C3"]
        end

        subgraph KO ["⚠️ 4 consumers / 3 partitions"]
            direction LR
            KP0["📦 P0"] --> KC1["📱 C1"]
            KP1["📦 P1"] --> KC2["📱 C2"]
            KP2["📦 P2"] --> KC3["📱 C3"]
            KC4["📱 C4<br/>(idle)"]
            KC4 -.->|"❌ pas de partition<br/>à consommer"| IDLE["💤 Inactif"]
        end
    end

    style OK fill:#e8f5e9,stroke:#2e7d32
    style KO fill:#fff3e0,stroke:#e65100
    style KC4 fill:#eceff1,stroke:#9e9e9e,stroke-dasharray:3 3
    style IDLE fill:#ffe0b2
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

### Points à retenir
- Les consumer groups sont le mécanisme principal de **scalabilité horizontale** côté lecture dans Kafka.
- Kafka n'est **pas une queue** : plusieurs consumers (ou groupes) peuvent lire les mêmes messages sans interférence.
- La scalabilité d'un consumer group est naturellement **bornée par le nombre de partitions** du topic (un consumer par partition au maximum dans un groupe).

---

##### Schéma de synthèse — Carte mentale Consumers (3 modules)

<div align="center">

```mermaid
mindmap
  root((📱 Consumers<br/>Kafka))
    Module 1 — Bases
      KafkaConsumer
      bootstrap.servers
      subscribe liste ou regex
      poll loop infinie
      ConsumerRecords
      Streaming continu
    Module 2 — Offsets
      Offset position
      Commit après traitement
      Topic __consumer_offsets
      Reprise après crash
      Commits batchés
        Par seuil
        Par temps
      Idempotence recommandée
    Module 3 — Groups
      Consumer group
        Partitions partagées
        Parallélisme
      Consumers indépendants
        Chacun lit tout
        Offsets séparés
      Limite
        1 consumer par partition
        Au max dans un groupe
      Scalabilité horizontale
```

</div>
