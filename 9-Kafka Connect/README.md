# Kafka Connect — Apache Kafka

## Table des matières
- [Module 1 : Kafka Connect](#module-1--kafka-connect)

---

## Module 1 : Kafka Connect

### Sujet

Ce module introduit **Kafka Connect**, le sous-système d'intégration d'Apache Kafka, conçu pour connecter Kafka à des systèmes externes (bases de données relationnelles, applications SaaS, moteurs de recherche, etc.) de manière standardisée, déclarative et scalable.

---

### Concepts clés

#### Pourquoi Kafka Connect ?

Dans la réalité des systèmes d'information, de nombreux systèmes ne sont pas Kafka. Il est pourtant nécessaire de :
- **Ingérer des données** depuis ces systèmes externes vers des topics Kafka.
- **Exporter des données** depuis des topics Kafka vers ces systèmes externes.

Kafka Connect est l'**API d'intégration officielle de Kafka** pour répondre à ce besoin. C'est à la fois :
- Un **écosystème de connecteurs enfichables** (*pluggable connectors*) configurables de manière déclarative.
- Un **système distribué client** qui exécute ces connecteurs en produisant vers Kafka ou en consommant depuis Kafka.

Comme tout composant non-broker dans l'écosystème Kafka, Connect est un **producteur et/ou consommateur**.

#### Source Connector vs Sink Connector

```mermaid
graph TB
    subgraph Source["SOURCE CONNECTOR<br/>Système externe → Kafka"]
        SysA["Système externe<br/>(DB, API, Files, etc.)"]
        SourceC["Source Connector<br/>(Reader)"]
        KafkaSrc["Topic Kafka<br/>(Events)"]
        SysA -->|Lecture| SourceC
        SourceC -->|Produit| KafkaSrc
    end
    
    subgraph Sink["SINK CONNECTOR<br/>Kafka → Système externe"]
        KafkaSnk["Topic Kafka<br/>(Events)"]
        SinkC["Sink Connector<br/>(Writer)"]
        SysB["Système externe<br/>(ElasticSearch, DB, Cloud, etc.)"]
        KafkaSnk -->|Consomme| SinkC
        SinkC -->|Écrit| SysB
    end
    
    style Source fill:#e3f2fd
    style Sink fill:#f3e5f5
```

| Type                 | Direction               | Description                                                                         |
|----------------------|-------------------------|-------------------------------------------------------------------------------------|
| **Source Connector** | Système externe → Kafka | Lit depuis une source externe (ex. base de données) et produit dans un topic Kafka  |
| **Sink Connector**   | Kafka → Système externe | Consomme depuis un topic Kafka et écrit dans un système externe (ex. Elasticsearch) |

---

### Fonctionnement

#### Architecture

```mermaid
graph TB
    subgraph External1["Système A<br/>(PostgreSQL)"]
        DB1[(Database)]
    end
    
    subgraph KafkaCluster["Apache Kafka Cluster"]
        Broker1["Broker 1"]
        Broker2["Broker 2"]
        Topic1["Topic: users-changes"]
        Topic2["Topic: product-sync"]
        Broker1 --> Topic1
        Broker2 --> Topic2
    end
    
    subgraph ConnectCluster["Kafka Connect Cluster<br/>(Distributed System)"]
        ConnectWorker1["Worker 1<br/>Source Connector"]
        ConnectWorker2["Worker 2<br/>Sink Connector"]
        ConnectWorker3["Worker 3<br/>Standby"]
        ConnectWorker1 -.->|Coordonnation| ConnectWorker2
        ConnectWorker2 -.->|Coordonnation| ConnectWorker3
    end
    
    subgraph External2["Système B<br/>(Elasticsearch)"]
        ES[(Index: users)]
    end
    
    DB1 -->|CDC| ConnectWorker1
    ConnectWorker1 -->|Produit| Topic1
    Topic2 -->|Consomme| ConnectWorker2
    ConnectWorker2 -->|Indexe| ES
    
    style External1 fill:#fff3e0
    style External2 fill:#fff3e0
    style KafkaCluster fill:#e3f2fd
    style ConnectCluster fill:#f3e5f5
```

- Connect s'exécute **en dehors des brokers Kafka**, comme une application cliente.
- Il peut fonctionner en **instance unique** ou en **cluster de plusieurs instances** pour la tolérance aux pannes et la scalabilité.
- Les messages **transitent par Kafka** entre systèmes externes : un source connector ingère depuis un système A, un sink connector exporte vers un système B, Kafka servant de hub central.

#### Configuration déclarative

```mermaid
flowchart TD
    A["Définir la configuration JSON<br/>(connecteur, params, topic,<br/>sécurité, etc.)"]
    B["Envoyer au REST API<br/>POST /connectors"]
    C["Cluster Connect<br/>reçoit la config"]
    D{Classe<br/>accessible?}
    E["❌ Erreur<br/>JAR manquant"]
    F["✅ Charger le connecteur"]
    G["Créer les tasks"]
    H["Démarrer automatiquement"]
    I["Connecteur actif<br/>ingère/exporte les données"]
    
    A --> B
    B --> C
    C --> D
    D -->|Non| E
    D -->|Oui| F
    F --> G
    G --> H
    H --> I
    
    E -.->|Ajouter JAR| A
```

Kafka Connect repose sur une **configuration déclarative en JSON** : il n'est pas nécessaire d'écrire du code pour se connecter à un système comme Elasticsearch. Il suffit de :
1. Écrire un fichier JSON de configuration (paramètres de sécurité, adresse du cluster cible, topic source/destination, classe du connecteur, etc.).
2. Envoyer ce JSON à un **endpoint REST** du cluster Connect.
3. S'assurer que la classe du connecteur est accessible par l'instance Connect.

Le connecteur démarre alors automatiquement.

#### Single Message Transforms (SMT)

```mermaid
graph LR
    Input["Message brut<br/>{id, name, email,<br/>phone, ssn}"]
    
    SMT1["SMT 1:<br/>Ajouter champ<br/>source_system<br/>= 'postgres'"]
    SMT2["SMT 2:<br/>Masquer PII<br/>(ssn, phone)"]
    SMT3["SMT 3:<br/>Renommer<br/>id → user_id"]
    SMT4["SMT 4:<br/>Filtrer<br/>si status != active"]
    
    Output["Message transformé<br/>{user_id, name, email,<br/>ssn: '***',<br/>source_system: 'postgres'}"]
    
    Input --> SMT1
    SMT1 --> SMT2
    SMT2 --> SMT3
    SMT3 --> SMT4
    SMT4 --> Output
    
    style Input fill:#fff3e0
    style Output fill:#e8f5e9
    style SMT1 fill:#f3e5f5
    style SMT2 fill:#f3e5f5
    style SMT3 fill:#f3e5f5
    style SMT4 fill:#f3e5f5
```

Les connecteurs supportent des **transformations légères et sans état** (*stateless*) appliquées message par message, appelées **Single Message Transforms**. Exemples de transformations possibles :

- **Filtrer** des messages selon un critère
- **Ajouter des champs** de contexte (ex. ajouter un champ `source_system` avec la valeur `users_db` à chaque message issu d'une table Postgres)
- **Renommer** des champs
- **Masquer des données PII** (*Personally Identifiable Information*)
- **Modifier des valeurs** de champs
- **Extraire une valeur** du message pour en faire la clé du message Kafka

#### SMT vs Stream Processing

```mermaid
graph TB
    SMT["<b>Single Message Transforms</b><br/>Léger & Stateless<br/>Per-message transformations"]
    
    SPE["<b>Stream Processing</b><br/>(Flink, Kafka Streams)<br/>Stateful & Complex<br/>Jointures, Agrégations, etc."]
    
    Use1["✅ Filtrage simple"]
    Use2["✅ Enrichissement"]
    Use3["✅ Renommage"]
    
    Use4["❌ Jointures"]
    Use5["❌ Agrégations"]
    Use6["❌ Fenêtres temporelles"]
    
    SMT --> Use1
    SMT --> Use2
    SMT --> Use3
    
    SPE --> Use4
    SPE --> Use5
    SPE --> Use6
    
    style SMT fill:#f3e5f5
    style SPE fill:#e3f2fd
```

> **Important** : Les SMT sont **strictement stateless**. Toute transformation nécessitant un état (jointures, agrégations, etc.) doit être réalisée en aval avec un moteur de stream processing dédié comme **Apache Flink** ou **Kafka Streams**, une fois les données présentes dans Kafka.

---

### Écosystème de connecteurs

```mermaid
graph TB
    Ecosystem["<b>Écosystème Kafka Connect</b><br/>+4000 connecteurs"]
    
    subgraph Distribution["Distribution Pareto (Loi de puissance)"]
        Top10["5-10%<br/>Connecteurs populaires<br/>(PostgreSQL, MySQL, ES, S3, etc.)"]
        Coverage["= 90% des besoins<br/>réels en production"]
        
        Tail["90-95%<br/>Longue traîne<br/>(cas spécialisés)"]
        Coverage2["= 10% des besoins<br/>(moins maintenus)"]
        
        Top10 --> Coverage
        Tail --> Coverage2
    end
    
    subgraph Sources["Sources officielles"]
        GitHub["GitHub<br/>4000+ connecteurs<br/>(variabilité de maintenance)"]
        Hub["Confluent Hub<br/>hub.confluent.io<br/>100+ connecteurs<br/>Confluent-supportés"]
        Cloud["Confluent Cloud<br/>80+ connecteurs<br/>Entièrement managés<br/>(sans opération)"]
    end
    
    Ecosystem --> Distribution
    Ecosystem --> Sources
    
    style Distribution fill:#f3e5f5
    style Sources fill:#e3f2fd
    style Top10 fill:#c8e6c9
    style Hub fill:#fff9c4
    style Cloud fill:#b2dfdb
```

L'un des grands avantages de Kafka Connect est son **vaste écosystème de connecteurs** développés, testés et maintenus par la communauté et les éditeurs.

- **Loi de puissance** (*power law distribution*) : 5 à 10 % des connecteurs disponibles couvrent 90 % des besoins réels d'intégration. Les cas d'usage courants sont donc très bien couverts par des connecteurs éprouvés en production.
- **Longue traîne** : Plus de **4 000 connecteurs** sont disponibles sur GitHub, dans des états de maturité variables. Les connecteurs moins maintenus peuvent fonctionner pour certains cas mais ne sont pas garantis.
- **Confluent Hub** (`hub.confluent.io`) : Catalogue centralisé de connecteurs référencés par Confluent. Confluent supporte plus de **100 connecteurs pré-construits**, dont plus de **80 connecteurs entièrement managés** dans Confluent Cloud (aucune gestion de cluster Connect nécessaire).

---

### Cas d'usage

```mermaid
graph TB
    KC["Kafka Connect<br/>Hub central"]
    
    subgraph CDC["Change Data Capture"]
        CDC1["Source: PostgreSQL"]
        CDC2["Source: MySQL"]
        CDC3["Topic: db-changes"]
        CDC1 -->|CDC| KC
        CDC2 -->|CDC| KC
        KC --> CDC3
    end
    
    subgraph Search["Indexation Moteur Recherche"]
        KC2["Kafka Connect"]
        Topic["Topic: user-events"]
        ES["Sink: Elasticsearch"]
        Topic --> KC2
        KC2 --> ES
    end
    
    subgraph SaaS["Intégration SaaS"]
        KC3["Kafka Connect"]
        Topic3["Topic: crm-data"]
        Salesforce["Sink: Salesforce"]
        Topic3 --> KC3
        KC3 --> Salesforce
    end
    
    subgraph Pipeline["Pipeline Multi-systèmes"]
        S1["System A<br/>(PostgreSQL)"]
        S2["System B<br/>(Data Lake)"]
        S3["System C<br/>(Analytics)"]
        KCPipe["Kafka Connect"]
        S1 -->|Source| KCPipe
        KCPipe -->|Sink| S2
        KCPipe -->|Sink| S3
    end
    
    style CDC fill:#fff3e0
    style Search fill:#f3e5f5
    style SaaS fill:#e8f5e9
    style Pipeline fill:#e3f2fd
```

- **Change Data Capture (CDC)** : capturer les modifications d'une base de données relationnelle (ex. Postgres) et les produire dans un topic Kafka.
- **Indexation dans un moteur de recherche** : consommer depuis un topic Kafka et écrire dans Elasticsearch.
- **Intégration avec des applications SaaS** : synchroniser des données entre Kafka et des outils tiers.
- **Pipelines de données multi-systèmes** : faire transiter des données entre plusieurs systèmes hétérogènes en utilisant Kafka comme hub central.

---

### Mises en garde

```mermaid
graph TB
    subgraph Dangers["⚠️ Pièges courants"]
        D1["SMT stateless<br/>❌ Jointures<br/>❌ Agrégations<br/>❌ État complexe<br/>✅ Filtrage simple"]
        D2["Connecteurs longue traîne<br/>❌ Maintenance faible<br/>❌ Production risqué<br/>✅ Vérifier avant adoption"]
        D3["Opération Connect<br/>❌ Charge opérationnelle<br/>❌ Clustering complexe<br/>✅ Managed Cloud alternative"]
    end
    
    Solutions["Solutions recommandées"]
    
    Fix1["→ Kafka Streams<br/>ou Flink"]
    Fix2["→ Utiliser<br/>Confluent Hub"]
    Fix3["→ Confluent Cloud<br/>ou Managed Service"]
    
    D1 --> Solutions
    D2 --> Solutions
    D3 --> Solutions
    Solutions --> Fix1
    Solutions --> Fix2
    Solutions --> Fix3
    
    style Dangers fill:#ffebee
    style Solutions fill:#f3e5f5
    style D1 fill:#ffcdd2
    style D2 fill:#ffcdd2
    style D3 fill:#ffcdd2
```

- Les **Single Message Transforms sont stateless** : ne pas tenter d'y implémenter une logique stateful (jointures, comptages, etc.) — utiliser Flink ou Kafka Streams à la place.
- Les connecteurs de la **longue traîne** (peu maintenus sur GitHub) peuvent ne pas convenir à tous les cas d'usage : vérifier leur niveau de maintenance avant adoption.
- Opérer un cluster Connect soi-même implique une **charge opérationnelle** : une alternative est d'utiliser les connecteurs managés d'un service cloud (ex. Confluent Cloud).

---

### Points à retenir

```mermaid
graph TB
    KC["<b>Kafka Connect</b><br/>Standard d'intégration Kafka"]
    
    Core["Fondamentaux"]
    Ecosystem["Écosystème"]
    Operations["Opérations"]
    
    C1["Système distribué client<br/>(externe aux brokers)"]
    C2["Configuration déclarative JSON<br/>(zéro code)"]
    C3["Source ← → Sink<br/>(bidirectionnel)"]
    
    E1["4000+ connecteurs GitHub<br/>(variabilité maintenance)"]
    E2["100+ connecteurs Confluent<br/>(supportés)"]
    E3["80+ connecteurs Cloud<br/>(fully managed)"]
    
    O1["SMT: stateless<br/>léger & simple"]
    O2["Scalable: multi-workers<br/>tolérance pannes"]
    O3["Incontournable:<br/>tout système non-trivial"]
    
    KC --> Core
    KC --> Ecosystem
    KC --> Operations
    
    Core --> C1
    Core --> C2
    Core --> C3
    
    Ecosystem --> E1
    Ecosystem --> E2
    Ecosystem --> E3
    
    Operations --> O1
    Operations --> O2
    Operations --> O3
    
    style KC fill:#e3f2fd
    style Core fill:#c8e6c9
    style Ecosystem fill:#fff9c4
    style Operations fill:#f3e5f5
```

- Kafka Connect est le **standard d'intégration de Kafka** avec les systèmes externes.
- Il fonctionne comme un **système distribué client** (producteur/consommateur) externe aux brokers.
- Deux types de connecteurs : **source** (externe → Kafka) et **sink** (Kafka → externe).
- La configuration est **déclarative en JSON**, sans nécessité d'écrire du code d'intégration.
- Les **Single Message Transforms** permettent des transformations légères et stateless directement dans le connecteur.
- L'écosystème compte plus de **4 000 connecteurs** sur GitHub et plus de **100 connecteurs supportés** par Confluent.
- Confluent Cloud propose plus de **80 connecteurs entièrement managés**, sans gestion d'infrastructure Connect.
- Kafka Connect est **incontournable** dans tout système Kafka non trivial nécessitant des échanges avec des systèmes tiers.