# Introduction à Apache Kafka — Tim Berglund, Confluent

## Table des matières
- [Module 1 : Introduction à Apache Kafka et à la pensée événementielle](#module-1--introduction-à-apache-kafka-et-à-la-pensée-événementielle)

---

## Module 1 : Introduction à Apache Kafka et à la pensée événementielle

### Sujet
Ce module présente Apache Kafka, son rôle dans les systèmes de données modernes, et le changement de paradigme fondamental qu'il implique : passer d'une pensée orientée *choses* (tables, entités) à une pensée orientée *événements* (faits horodatés).

---

### Concepts clés

#### Apache Kafka
Kafka est une infrastructure logicielle devenue la **fondation universelle** sur laquelle les systèmes de données modernes sont construits. Il s'adresse à trois types d'usages principaux :
- Les **pipelines de données et l'analytique**
- La **connexion de microservices** dans des architectures applicatives
- Le **transfert de données** depuis des systèmes tiers vers un système cible

##### Schéma — Les trois grands usages de Kafka

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        K[("Apache Kafka<br/>Fondation des systèmes<br/>de données modernes")]
        K --> U1["📊 Pipelines de données<br/>& Analytique"]
        K --> U2["🔗 Connexion de<br/>Microservices"]
        K --> U3["🔄 Transfert de données<br/>depuis systèmes tiers"]

        U1 --> R1["Tableaux de bord<br/>BI, ML, reporting"]
        U2 --> R2["Architecture<br/>distribuée découplée"]
        U3 --> R3["Synchronisation<br/>cross-systèmes"]
    end

    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style U1 fill:#43a047,color:#fff
    style U2 fill:#fb8c00,color:#fff
    style U3 fill:#8e24aa,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Événement (*Event*)
Un événement est un **fait qui se produit à un instant précis dans le temps**. Ce n'est pas une représentation d'une entité statique, mais la capture d'une action ou d'un changement. Exemples :
- Un article vendu dans un magasin
- Un conducteur de voiture connectée activant son clignotant
- Un utilisateur qui clique sur une page web

Les événements forment **l'épine dorsale des systèmes de données contemporains**.

##### Schéma — L'anatomie d'un événement

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        E["⚡ Événement<br/>= Fait horodaté"]
        E --> A1["🛒 Article vendu<br/>en magasin"]
        E --> A2["🚗 Clignotant activé<br/>voiture connectée"]
        E --> A3["🖱️ Clic utilisateur<br/>page web"]

        A1 --> T["⏱️ Timestamp<br/>(instant T)"]
        A2 --> T
        A3 --> T
    end

    style E fill:#e53935,color:#fff,stroke:#b71c1c,stroke-width:3px
    style T fill:#fdd835,color:#000,stroke:#f57f17,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Table vs Événement
Le paradigme traditionnel des bases de données pousse à penser en termes de **tables**, qui stockent des représentations d'entités du monde réel (articles en inventaire, voitures connectées, utilisateurs inscrits). Kafka encourage à inverser cette logique : plutôt que de stocker l'état d'une chose, on capture **ce qui se passe**, instant par instant.

##### Schéma — Le changement de paradigme

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph TRAD ["🗄️ Paradigme Traditionnel — Tables"]
            direction TB
            T1["Entité : Article"]
            T2["État stocké : 42 unités"]
            T3["Lecture ponctuelle"]
            T1 --> T2 --> T3
        end

        subgraph KAFKA ["⚡ Paradigme Kafka — Événements"]
            direction TB
            E1["Fait : Vente à 14h32"]
            E2["Fait : Vente à 14h35"]
            E3["Fait : Réapprovisionnement à 15h00"]
            E1 --> E2 --> E3
        end

        TRAD -.->|"Inversion<br/>de la logique"| KAFKA
    end

    style TRAD fill:#ffebee,stroke:#c62828
    style KAFKA fill:#e8f5e9,stroke:#2e7d32
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

#### Traitement en temps réel
Kafka est conçu pour traiter les événements **au moment où ils se produisent**, sans les accumuler dans des tables ou des fichiers pour un traitement différé (batch). Le principe est clair : dès qu'un événement survient, le travail de traitement est effectué **immédiatement**. Il n'y a pas de logique du type "on stocke maintenant, on traite plus tard".

##### Schéma — Batch vs Temps réel

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph BATCH ["⏳ Approche Batch (traditionnelle)"]
            direction LR
            B1["Événement 1"] --> BS[("Stockage<br/>fichiers / DB")]
            B2["Événement 2"] --> BS
            B3["Événement N"] --> BS
            BS -.->|"⏰ Plus tard..."| BP["Traitement<br/>en lot"]
            BP --> BR["Résultats"]
        end

        subgraph STREAM ["⚡ Approche Kafka (temps réel)"]
            direction LR
            S1["Événement"] --> SK[("Kafka")]
            SK -->|"Immédiat"| SP["Traitement"]
            SP --> SR["Résultats<br/>en continu"]
        end
    end

    style BATCH fill:#fff3e0,stroke:#e65100
    style STREAM fill:#e3f2fd,stroke:#0d47a1
    style BS fill:#ffab91
    style SK fill:#64b5f6,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Stockage et mémoire des événements
Bien que Kafka soit orienté temps réel, il **conserve en mémoire les événements passés**. Cette capacité de rétention permet de rejouer des événements, d'alimenter des systèmes en retard, ou de reconstruire un état. Kafka n'est donc pas seulement un bus de messages éphémère.

##### Schéma — Cycle de vie d'un événement dans Kafka

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant P as 🏭 Producteur
    participant K as ⚡ Kafka<br/>(Broker)
    participant M as 💾 Mémoire<br/>(rétention)
    participant C1 as 📱 Consommateur 1<br/>(temps réel)
    participant C2 as 🔄 Consommateur 2<br/>(rejeu différé)

    rect rgb(250, 250, 250)
        P->>K: Publication événement à T
        K->>M: Conservation
        K->>C1: Diffusion immédiate
        Note over C1: Traitement en temps réel

        Note over M: ⏱️ Temps passe...

        C2->>K: Demande de rejeu
        M->>K: Récupération événements passés
        K->>C2: Rejeu d'événements
        Note over C2: Reconstruction d'état<br/>ou rattrapage
    end
```

</div>

---

#### Gestion du schéma (*Schema*)
Kafka intègre des mécanismes pour gérer le **schéma des événements** qu'il stocke. Cela correspond à une logique plus proche de la pensée "chose/entité" : on définit la structure des données circulant dans Kafka, ce qui facilite la gouvernance et la compatibilité entre producteurs et consommateurs.

##### Schéma — Gouvernance via le Schema Registry

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P1["🏭 Producteur A"] -->|"écrit selon<br/>schéma v1"| K[("⚡ Kafka")]
        P2["🏭 Producteur B"] -->|"écrit selon<br/>schéma v2"| K

        SR["📋 Schema Registry<br/>(gouvernance)"] -.->|"valide"| P1
        SR -.->|"valide"| P2
        SR -.->|"valide"| C1
        SR -.->|"valide"| C2

        K -->|"lit avec<br/>compatibilité"| C1["📱 Consommateur X"]
        K -->|"lit avec<br/>compatibilité"| C2["📱 Consommateur Y"]
    end

    style SR fill:#7b1fa2,color:#fff,stroke:#4a148c,stroke-width:2px
    style K fill:#1e88e5,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

- **Stream processing** : réaliser des calculs en temps réel sur des flux d'événements
- **Gouvernance des données événementielles** : imposer des règles et des schémas sur les données qui transitent
- **Connectivité inter-systèmes** : relier Kafka à des systèmes tiers non-Kafka via des connecteurs
- **Pipelines à très haute volumétrie** : des entreprises traitent des millions d'événements par seconde, des milliards par heure, des trillions par jour

##### Schéma — Échelle de volumétrie de Kafka

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        S["⏱️ Seconde"] -->|"×3 600"| H["⏰ Heure"]
        H -->|"×24"| D["📅 Jour"]

        S -.->|"📊"| SV["Millions<br/>d'événements/s"]
        H -.->|"📊"| HV["Milliards<br/>d'événements/h"]
        D -.->|"📊"| DV["Trillions<br/>d'événements/j"]
    end

    style SV fill:#4caf50,color:#fff
    style HV fill:#ff9800,color:#fff
    style DV fill:#f44336,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Comparaisons

| Paradigme traditionnel (Table)    | Paradigme Kafka (Événement)            |
|-----------------------------------|----------------------------------------|
| Stocke l'état actuel d'une entité | Capture un fait survenu à un instant T |
| Traitement différé (batch)        | Traitement immédiat (temps réel)       |
| Données au repos                  | Données en mouvement                   |
| Lecture ponctuelle                | Flux continu                           |

##### Schéma — Données au repos vs Données en mouvement

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph REST ["🛌 Données au repos (Tables)"]
            direction TB
            DB[("🗄️ Base de données")]
            DB --- Q1["SELECT * FROM ..."]
            DB --- Q2["État figé à un instant"]
        end

        subgraph MOTION ["🌊 Données en mouvement (Kafka)"]
            direction LR
            FLOW1["📤"] -.->|"événement"| FLOW2["📥"]
            FLOW2 -.->|"événement"| FLOW3["📥"]
            FLOW3 -.->|"événement"| FLOW4["📥"]
        end
    end

    style REST fill:#eceff1,stroke:#455a64
    style MOTION fill:#e0f7fa,stroke:#006064
    style DB fill:#90a4ae,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage — Écosystème et plateforme

Au-delà du broker Kafka lui-même, un **écosystème complet** — appelé *data streaming platform* — s'est constitué autour de Kafka :
- Outils de **stream processing** pour transformer et enrichir les événements à la volée
- Composants de **gouvernance** pour contrôler la qualité et la conformité des données
- **Connecteurs** pour intégrer Kafka avec des systèmes externes
- Services cloud-natifs comme **Confluent Cloud**, qui offrent Kafka en tant que service managé, avec des différences notables par rapport à Apache Kafka open source

##### Schéma — La Data Streaming Platform autour de Kafka

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph DSP ["🌐 Data Streaming Platform"]
            K[("⚡ Apache Kafka<br/>Broker central")]

            SP["🔧 Stream Processing<br/>Kafka Streams / Flink<br/>Transformer & enrichir"]
            GOV["📋 Gouvernance<br/>Schema Registry<br/>Qualité & conformité"]
            CON["🔌 Connecteurs<br/>Kafka Connect<br/>Intégration externe"]
            CC["☁️ Confluent Cloud<br/>Service managé"]

            K <--> SP
            K <--> GOV
            K <--> CON
            K -.-> CC
        end

        EXT1[("🗄️ Bases de données<br/>externes")] -->|"source"| CON
        EXT2[("📦 Systèmes legacy")] -->|"source"| CON
        CON -->|"sink"| EXT3[("📊 Data Warehouse")]
        CON -->|"sink"| EXT4[("☁️ Cloud Storage")]
    end

    style K fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style DSP fill:#f5f5f5,stroke:#616161,stroke-width:2px
    style SP fill:#43a047,color:#fff
    style GOV fill:#7b1fa2,color:#fff
    style CON fill:#fb8c00,color:#fff
    style CC fill:#039be5,color:#fff
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Points à retenir

- Kafka est la **fondation des systèmes de données modernes**, utilisée par de nombreuses grandes entreprises à l'échelle mondiale.
- Le changement de paradigme essentiel : penser **événements** plutôt que **choses/tables**.
- Kafka traite les événements **en temps réel**, mais peut aussi les **conserver** pour un accès ultérieur.
- La gestion du **schéma** dans Kafka est possible et importante pour la gouvernance.
- Il existe une distinction entre **Apache Kafka** (open source) et les **services Kafka cloud-natifs** comme Confluent Cloud.
- Kafka est une porte d'entrée vers un écosystème plus large : stream processing, gouvernance, connecteurs — un domaine vaste qui ne fait que commencer.

##### Schéma de synthèse — Carte mentale du Module 1

<div align="center">

```mermaid
mindmap
  root((⚡ Apache<br/>Kafka))
    Usages
      Pipelines & Analytique
      Microservices
      Transfert de données
    Concepts clés
      Événement
        Fait horodaté
        Action capturée
      Table vs Événement
        Inversion paradigme
        Choses → Faits
    Fonctionnement
      Temps réel
        Traitement immédiat
        Pas de batch
      Rétention
        Mémoire des événements
        Rejeu possible
      Schema
        Gouvernance
        Compatibilité
    Écosystème
      Stream Processing
      Schema Registry
      Connecteurs
      Confluent Cloud
    Échelle
      Millions évts / sec
      Milliards évts / h
      Trillions évts / jour
```

</div>
