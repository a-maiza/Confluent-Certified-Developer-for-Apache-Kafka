# Apache Kafka — Les Topics

## Table des matières
- [Module 1 : Les Topics Kafka](#module-1--les-topics-kafka)

---

## Module 1 : Les Topics Kafka

### Sujet
Ce module introduit le concept fondamental de **topic** dans Kafka, en le comparant au modèle tabulaire traditionnel des bases de données. Il explique la structure des messages, les propriétés des logs immuables, et les implications pratiques de ce paradigme pour la modélisation des données.

---

### Concepts clés

#### Table vs Log
Dans une base de données traditionnelle, les données sont stockées dans des **tables**. Chaque entité du monde réel (ex. : un thermostat) occupe une ligne, et quand l'état de cette entité change, on **met à jour la ligne**. Le problème : on **perd le contexte historique**. Si la température de la cuisine passe de 22°C à 24°C, la valeur précédente disparaît. Il devient impossible de répondre à des questions comme : à quelle vitesse la cuisine chauffe-t-elle ? À quelle heure de la journée ?

Kafka repose sur un modèle radicalement différent : le **log**. Un log est une **séquence ordonnée de messages**. On y ajoute des éléments à la fin, et on ne modifie jamais ce qui y a déjà été écrit. Chaque changement dans le monde est enregistré comme un **nouvel événement**, préservant ainsi tout l'historique.

##### Schéma — Table (UPDATE) vs Log (APPEND)

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph TABLE ["🗄️ Table (BDD traditionnelle)"]
            direction TB
            T1["Cuisine : 22°C"] -->|"UPDATE"| T2["Cuisine : 24°C"]
            T2 -->|"UPDATE"| T3["Cuisine : 25°C"]
            T3 -.->|"❌ Historique perdu"| TX["État final<br/>uniquement"]
        end

        subgraph LOG ["📜 Log Kafka (append-only)"]
            direction TB
            L1["[0] 22°C @ 08h00"]
            L2["[1] 24°C @ 08h15"]
            L3["[2] 25°C @ 08h30"]
            L4["[3] 26°C @ 08h45"]
            L1 --> L2 --> L3 --> L4
            L4 -.->|"✅ Historique complet"| LX["Toutes les valeurs<br/>conservées"]
        end
    end

    style TABLE fill:#ffebee,stroke:#c62828
    style LOG fill:#e8f5e9,stroke:#2e7d32
    style TX fill:#ef9a9a,color:#000
    style LX fill:#a5d6a7,color:#000
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Topic
Dans Kafka, les logs s'appellent des **topics**. Un topic est l'endroit où sont stockés les messages. On peut en avoir des milliers dans un même cluster Kafka, chacun dédié à un type de données ou à un usage particulier — à l'image des tables dans une base de données.

##### Schéma — Un cluster Kafka contient plusieurs topics

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        CLUSTER[("⚡ Cluster Kafka")]
        CLUSTER --> T1["📋 topic: thermostat-readings"]
        CLUSTER --> T2["📋 topic: user-clicks"]
        CLUSTER --> T3["📋 topic: orders"]
        CLUSTER --> T4["📋 topic: payments"]
        CLUSTER --> TN["📋 topic: ... (milliers)"]
    end

    style CLUSTER fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style T1 fill:#fff8e1,stroke:#f57c00
    style T2 fill:#fff8e1,stroke:#f57c00
    style T3 fill:#fff8e1,stroke:#f57c00
    style T4 fill:#fff8e1,stroke:#f57c00
    style TN fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:3 3
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Immutabilité des messages
C'est une caractéristique **définissante** de Kafka : une fois qu'un message est écrit dans un topic, il est **immuable**. On ne peut pas le modifier. On peut l'oublier (le supprimer après expiration), mais pas le changer. Un événement s'est produit, il fait partie de l'histoire.

##### Schéma — Le principe d'immutabilité

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur"] -->|"écrit"| M["📦 Message<br/>offset: 42<br/>value: 24°C"]
        M --> T[("📋 Topic")]

        T -.->|"✅ Lecture<br/>(non destructive)"| OK1["📖 Possible"]
        T -.->|"⏳ Expiration<br/>(rétention)"| OK2["🗑️ Possible"]
        T -.->|"✏️ Modification"| KO["❌ IMPOSSIBLE"]
    end

    style P fill:#43a047,color:#fff
    style M fill:#fff8e1,stroke:#f57c00,stroke-width:2px
    style T fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style OK1 fill:#c8e6c9,color:#000
    style OK2 fill:#fff9c4,color:#000
    style KO fill:#ffcdd2,color:#000,stroke:#c62828,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Log vs Queue
Un topic Kafka n'est **pas une queue**. La différence est fondamentale :
- Dans une **queue**, lire un message le consomme et le retire : plus personne ne peut le lire.
- Dans un **log**, lire un message ne le supprime pas : il reste disponible, peut être relu, et d'autres consommateurs peuvent y accéder indépendamment.

Cette distinction a des implications très larges sur l'architecture des systèmes.

##### Schéma — Queue (consommation destructive) vs Log Kafka (lecture partagée)

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph QUEUE ["📤 Queue (RabbitMQ, SQS...)"]
            direction LR
            QM1["Msg 1"] --> QM2["Msg 2"] --> QM3["Msg 3"]
            QM1 -->|"lit & retire"| QC["Consommateur"]
            QM1 -.->|"❌ disparu après lecture"| QGONE["Indisponible"]
        end

        subgraph LOG ["📜 Log Kafka"]
            direction LR
            LM1["Msg 1"] --> LM2["Msg 2"] --> LM3["Msg 3"]
            LM1 -->|"lit (offset 0)"| LC1["Consommateur A"]
            LM1 -->|"lit (offset 0)"| LC2["Consommateur B"]
            LM1 -->|"relit (offset 0)"| LC3["Consommateur C"]
            LM1 -.->|"✅ reste disponible"| LSTILL["Toujours là"]
        end
    end

    style QUEUE fill:#ffebee,stroke:#c62828
    style LOG fill:#e8f5e9,stroke:#2e7d32
    style QGONE fill:#ef9a9a,color:#000
    style LSTILL fill:#a5d6a7,color:#000
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

#### Ajout de messages
Les messages sont toujours **ajoutés à la fin** du topic (append-only). Dans l'exemple du thermostat, chaque nouvelle lecture de capteur génère un nouveau message dans le topic `thermostat-readings`. Si la cuisine passe de 22°C à 24°C, un nouveau message est ajouté — l'ancien reste intact. On dispose ainsi de **l'historique complet** des relevés.

##### Schéma — Append-only sur un topic thermostat

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        S["🌡️ Capteur<br/>thermostat"] -->|"22°C @ 08h00"| K[("⚡ Kafka")]
        S -->|"24°C @ 08h15"| K
        S -->|"25°C @ 08h30"| K

        subgraph TOPIC ["📋 topic: thermostat-readings"]
            direction LR
            O0["offset 0<br/>22°C"] --> O1["offset 1<br/>24°C"] --> O2["offset 2<br/>25°C"] --> ONEXT["offset 3<br/>(à venir →)"]
        end

        K --> TOPIC
    end

    style S fill:#fb8c00,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style O0 fill:#e3f2fd,stroke:#1976d2
    style O1 fill:#e3f2fd,stroke:#1976d2
    style O2 fill:#e3f2fd,stroke:#1976d2
    style ONEXT fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:3 3
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Schema et format
Kafka est **totalement agnostique au format** des données. Internalement, les messages sont de simples **tableaux d'octets** (*bytes*). Kafka ne connaît pas le schéma. En pratique, on utilise des formats comme :
- **JSON** (lisible, souvent utilisé en développement)
- **Avro** (compact, avec gestion de schéma)
- **Protocol Buffers** (Protobuf)
- Tout format de sérialisation personnalisé

La gestion des schémas se fait en dehors du broker Kafka, via des outils dédiés (ex. : Schema Registry).

##### Schéma — Sérialisation et formats

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        APP["💻 Application"] --> SER["🔧 Sérialiseur"]
        SER --> F1["📄 JSON"]
        SER --> F2["📦 Avro"]
        SER --> F3["⚡ Protobuf"]
        SER --> F4["🛠️ Custom"]

        F1 --> BYTES["🔢 byte[]"]
        F2 --> BYTES
        F3 --> BYTES
        F4 --> BYTES

        BYTES --> K[("⚡ Kafka<br/>(agnostique)")]

        SR["📋 Schema Registry<br/>(externe)"] -.->|"valide"| SER
    end

    style APP fill:#43a047,color:#fff
    style SER fill:#fb8c00,color:#fff
    style BYTES fill:#9e9e9e,color:#fff
    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style SR fill:#7b1fa2,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Transformation de topics immuables
Puisque les messages sont immuables, pour transformer des données, on **crée de nouveaux topics**. Par exemple, pour ne garder que les relevés de température élevée, on lit le topic source, on applique un filtre (ex. via une requête SQL de stream processing), et on écrit les résultats dans un second topic `hot-locations`. C'est la manière standard de travailler avec des données immuables.

##### Schéma — Pipeline de transformation entre topics

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        T1[("📋 thermostat-readings<br/>22°C, 24°C, 30°C, 18°C, 35°C")]
        T1 --> SP["🔧 Stream Processor<br/>SELECT * WHERE temp > 28"]
        SP --> T2[("📋 hot-locations<br/>30°C, 35°C")]
        T2 --> SP2["🔧 Autre traitement<br/>(alertes, agrégats...)"]
        SP2 --> T3[("📋 alerts")]
    end

    style T1 fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style T2 fill:#e53935,color:#fff,stroke:#b71c1c,stroke-width:3px
    style T3 fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:3px
    style SP fill:#43a047,color:#fff
    style SP2 fill:#43a047,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Structure d'un message Kafka

Un message Kafka est composé des champs suivants :

| Champ         | Obligatoire             | Description                                                                                                                                                                                                      |
|---------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Value**     | ✅ Oui                   | Le contenu principal de l'événement — le *quoi*. Peut être JSON, Avro, Protobuf, une chaîne, un entier, etc.                                                                                                     |
| **Key**       | ❌ Non (mais recommandé) | Identifiant logique de l'entité concernée (ex. : ID du capteur, ID utilisateur). Utilisée pour distribuer les données efficacement entre les partitions du cluster. Doit être choisie avec soin.                 |
| **Timestamp** | Automatique             | Représente le *quand* de l'événement. Peut être défini par le producteur (temps de l'événement réel) ou assigné automatiquement par le broker à la réception.                                                    |
| **Headers**   | ❌ Non                   | Map de paires clé-valeur en chaînes de caractères non typées. Utilisée pour des **métadonnées légères** sur le message (ex. : source, contexte de traçabilité). Ne pas utiliser comme vecteur de données métier. |
| **Topic**     | Automatique             | Le topic auquel appartient le message.                                                                                                                                                                           |
| **Offset**    | Automatique             | Identifiant séquentiel du message dans le topic. Commence à 0, incrémenté de 1 à chaque nouveau message. Simple et immuable.                                                                                     |

##### Schéma — Anatomie d'un message Kafka

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        MSG["📦 Message Kafka"]
        MSG --> V["🎯 Value (✅ obligatoire)<br/>Le contenu — le 'quoi'<br/>JSON / Avro / Protobuf..."]
        MSG --> K["🔑 Key (recommandée)<br/>Identifiant logique<br/>→ partitionnement"]
        MSG --> TS["⏱️ Timestamp (auto)<br/>Le 'quand'<br/>producteur ou broker"]
        MSG --> H["🏷️ Headers (optionnel)<br/>Métadonnées légères<br/>clé-valeur string"]
        MSG --> T["📋 Topic (auto)<br/>Destination logique"]
        MSG --> O["#️⃣ Offset (auto)<br/>Position séquentielle<br/>0, 1, 2, 3, ..."]
    end

    style MSG fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style V fill:#43a047,color:#fff,stroke:#1b5e20,stroke-width:2px
    style K fill:#fb8c00,color:#fff
    style TS fill:#7b1fa2,color:#fff
    style H fill:#9e9e9e,color:#fff
    style T fill:#039be5,color:#fff
    style O fill:#e53935,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

- **Historisation continue** : capturer tous les états successifs d'un équipement (thermostat, capteur IoT) sans perdre l'historique
- **Audit trail** : conserver un journal immuable de toutes les actions effectuées dans un système
- **Fan-out** : plusieurs consommateurs lisent indépendamment le même topic sans interférence (impossible avec une queue)
- **Transformation en pipeline** : enchaîner des topics pour filtrer, enrichir ou agréger des données via le stream processing

##### Schéma — Le pattern Fan-out (un topic, plusieurs consommateurs indépendants)

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur"] --> T[("📋 topic: orders")]

        T --> C1["📊 Service Analytics<br/>(temps réel)"]
        T --> C2["💾 Service Archivage<br/>(stockage long terme)"]
        T --> C3["📧 Service Notifications<br/>(emails clients)"]
        T --> C4["💳 Service Facturation<br/>(génération factures)"]

        C1 -.->|"chacun avec son<br/>propre offset"| INDEP["🎯 Consommation<br/>indépendante"]
        C2 -.-> INDEP
        C3 -.-> INDEP
        C4 -.-> INDEP
    end

    style P fill:#43a047,color:#fff
    style T fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style C1 fill:#fff8e1,stroke:#f57c00
    style C2 fill:#fff8e1,stroke:#f57c00
    style C3 fill:#fff8e1,stroke:#f57c00
    style C4 fill:#fff8e1,stroke:#f57c00
    style INDEP fill:#e1bee7,stroke:#6a1b9a
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Mises en garde

- **Ne pas confondre topic et queue** : Kafka n'a pas de queues. Utiliser le terme "Kafka queue" est une erreur conceptuelle. Les topics sont des logs, et cette distinction a des conséquences architecturales majeures.
- **La clé doit être choisie avec soin** : elle détermine la distribution des messages entre les partitions. Un mauvais choix de clé peut créer des déséquilibres de charge dans le cluster.
- **L'immuabilité est absolue** : toute logique de "mise à jour" doit être repensée sous forme d'ajout d'un nouvel événement ou de création d'un topic transformé.
- **Kafka est schema-less** : sans outil de gestion de schéma (ex. Schema Registry), des incompatibilités entre producteurs et consommateurs peuvent survenir silencieusement.

##### Schéma — Les 4 pièges à éviter

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        WARN["⚠️ Pièges courants"]
        WARN --> P1["❌ Confondre topic et queue<br/><i>« Kafka queue » n'existe pas</i>"]
        WARN --> P2["❌ Mauvaise clé de partitionnement<br/><i>déséquilibre de charge</i>"]
        WARN --> P3["❌ Vouloir UPDATE un message<br/><i>impossible : ajouter un nouveau</i>"]
        WARN --> P4["❌ Ignorer la gestion de schéma<br/><i>incompatibilités silencieuses</i>"]
    end

    style WARN fill:#ff5722,color:#fff,stroke:#bf360c,stroke-width:3px
    style P1 fill:#ffcdd2,color:#000,stroke:#c62828
    style P2 fill:#ffcdd2,color:#000,stroke:#c62828
    style P3 fill:#ffcdd2,color:#000,stroke:#c62828
    style P4 fill:#ffcdd2,color:#000,stroke:#c62828
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Points à retenir

- Un **topic** est un log immuable et ordonné de messages — l'équivalent Kafka d'une table, mais fondamentalement différent dans son comportement.
- Les messages sont **immuables** : on ne les modifie jamais, on ajoute toujours à la fin.
- Un topic **n'est pas une queue** : les messages restent disponibles après lecture et peuvent être consommés plusieurs fois par des consommateurs distincts.
- La structure d'un message comprend : **value** (obligatoire), **key** (recommandée), **timestamp**, **headers**, **topic** et **offset**.
- Pour transformer des données, on crée de **nouveaux topics** à partir de l'existant — c'est le principe de base du stream processing.
- Kafka est **agnostique au format** : JSON, Avro, Protobuf ou tout autre format sont valables, la gestion du schéma étant externalisée.

##### Schéma de synthèse — Carte mentale du module Topics

<div align="center">

```mermaid
mindmap
  root((📋 Topics<br/>Kafka))
    Nature
      Log immuable
      Append-only
      Ordonné
      ≠ Queue
    Topic vs Table
      Table : UPDATE
        Perd l'historique
      Log : APPEND
        Conserve tout
    Message
      Value (obligatoire)
      Key (recommandée)
      Timestamp
      Headers
      Topic
      Offset
    Format
      Agnostique
      JSON
      Avro
      Protobuf
      Custom
    Transformation
      Nouveaux topics
      Stream processing
      Pipeline en chaîne
    Patterns
      Fan-out
      Audit trail
      Historisation
    Pièges
      Pas de queue
      Choix de la clé
      Immutabilité absolue
      Schema Registry
```

</div>
