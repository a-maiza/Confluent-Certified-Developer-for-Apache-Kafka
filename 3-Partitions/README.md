# Apache Kafka — Partitions

## Table des matières
- [Module 1 : Kafka Partitions — Scalabilité et distribution des messages](#module-1--kafka-partitions--scalabilité-et-distribution-des-messages)

---

## Module 1 : Kafka Partitions — Scalabilité et distribution des messages

### Sujet

Ce module introduit le concept de **partition** dans Apache Kafka, en expliquant pourquoi ce mécanisme est fondamental pour la scalabilité d'un système distribué, et comment il influence le routage et l'ordonnancement des messages.

---

### Concepts clés

#### Kafka en tant que système distribué

Kafka est conçu pour fonctionner sur un ensemble de machines (un cluster), tout en apparaissant comme un système unifié de l'extérieur. Cette architecture distribuée impose des contraintes sur la manière dont les données sont stockées et organisées.

##### Schéma — Cluster Kafka : vue interne vs vue externe

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph EXT ["👁️ Vue externe (clients)"]
            CLIENT["💻 Application cliente"] --> UNIFIED[("⚡ Kafka<br/>(système unifié)")]
        end

        subgraph INT ["🔧 Vue interne (réalité distribuée)"]
            direction LR
            B1[("🖥️ Broker 1")]
            B2[("🖥️ Broker 2")]
            B3[("🖥️ Broker 3")]
            B4[("🖥️ Broker N")]
            B1 <--> B2
            B2 <--> B3
            B3 <--> B4
            B1 <--> B3
        end

        EXT -.->|"abstraction"| INT
    end

    style CLIENT fill:#43a047,color:#fff
    style UNIFIED fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style B1 fill:#fff8e1,stroke:#f57c00
    style B2 fill:#fff8e1,stroke:#f57c00
    style B3 fill:#fff8e1,stroke:#f57c00
    style B4 fill:#fff8e1,stroke:#f57c00
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Partition

Une **partition** est une subdivision d'un topic Kafka. Au lieu de stocker l'intégralité des messages d'un topic sur une seule machine, Kafka découpe ce topic en plusieurs **logs indépendants** (les partitions), qui peuvent être répartis sur différents nœuds du cluster.

- Un topic peut avoir **des centaines ou des milliers de partitions**.
- Apache Kafka (autour de la version 4.0 au moment du cours) supporte environ **2 millions de partitions** au total dans un cluster.

##### Schéma — Un topic découpé en plusieurs partitions réparties sur le cluster

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        TOPIC["📋 Topic : orders"]

        subgraph CLUSTER ["🌐 Cluster Kafka"]
            direction LR
            subgraph BR1 ["🖥️ Broker 1"]
                P0["📦 Partition 0<br/>[m0, m1, m2...]"]
                P3["📦 Partition 3<br/>[m0, m1, m2...]"]
            end
            subgraph BR2 ["🖥️ Broker 2"]
                P1["📦 Partition 1<br/>[m0, m1, m2...]"]
                P4["📦 Partition 4<br/>[m0, m1, m2...]"]
            end
            subgraph BR3 ["🖥️ Broker 3"]
                P2["📦 Partition 2<br/>[m0, m1, m2...]"]
                P5["📦 Partition 5<br/>[m0, m1, m2...]"]
            end
        end

        TOPIC --> P0
        TOPIC --> P1
        TOPIC --> P2
        TOPIC --> P3
        TOPIC --> P4
        TOPIC --> P5
    end

    style TOPIC fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style P0 fill:#fff8e1,stroke:#f57c00
    style P1 fill:#fff8e1,stroke:#f57c00
    style P2 fill:#fff8e1,stroke:#f57c00
    style P3 fill:#fff8e1,stroke:#f57c00
    style P4 fill:#fff8e1,stroke:#f57c00
    style P5 fill:#fff8e1,stroke:#f57c00
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

#### Pourquoi partitionner ?

Sans partitionnement, un topic serait contraint de tenir entièrement sur **un seul nœud**. Cela imposerait une limite stricte à la capacité de stockage et de débit, égale à celle de la machine la plus grande du cluster. Le partitionnement supprime cette contrainte en permettant à un topic de s'étendre sur plusieurs machines.

##### Schéma — Sans partitions vs Avec partitions

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph WITHOUT ["❌ Sans partitionnement"]
            direction TB
            T1["📋 Topic complet"] --> N1[("🖥️ 1 seul nœud")]
            N1 -.->|"limite stricte"| LIMIT["💥 Capacité = 1 machine<br/>(stockage + débit)"]
        end

        subgraph WITH ["✅ Avec partitionnement"]
            direction TB
            T2["📋 Topic partitionné"]
            T2 --> NA[("🖥️ Nœud A")]
            T2 --> NB[("🖥️ Nœud B")]
            T2 --> NC[("🖥️ Nœud C")]
            T2 --> ND[("🖥️ Nœud N")]
            NA -.->|"scalabilité<br/>horizontale"| SCALE["🚀 Capacité = somme<br/>des machines"]
            NB -.-> SCALE
            NC -.-> SCALE
            ND -.-> SCALE
        end
    end

    style WITHOUT fill:#ffebee,stroke:#c62828
    style WITH fill:#e8f5e9,stroke:#2e7d32
    style LIMIT fill:#ef9a9a,color:#000
    style SCALE fill:#a5d6a7,color:#000
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Impact sur l'ordre des messages

Le partitionnement a un effet direct sur l'**ordonnancement des messages** :

- Il n'existe **pas d'ordre global garanti** au sein d'un topic partitionné.
- L'ordre n'est garanti qu'**au sein d'une même partition** : les messages écrits dans une partition donnée en sont lus dans le même ordre.

##### Schéma — Ordre garanti par partition, pas globalement

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph TOPIC ["📋 Topic : events (3 partitions)"]
            direction LR
            subgraph P0 ["📦 Partition 0"]
                direction LR
                A1["m1"] --> A2["m4"] --> A3["m7"]
            end
            subgraph P1 ["📦 Partition 1"]
                direction LR
                B1["m2"] --> B2["m5"] --> B3["m8"]
            end
            subgraph P2 ["📦 Partition 2"]
                direction LR
                C1["m3"] --> C2["m6"] --> C3["m9"]
            end
        end

        P0 -.->|"✅ ordre interne<br/>garanti"| OK1["m1 → m4 → m7"]
        P1 -.->|"✅ ordre interne<br/>garanti"| OK2["m2 → m5 → m8"]
        P2 -.->|"✅ ordre interne<br/>garanti"| OK3["m3 → m6 → m9"]

        TOPIC -.->|"❌ pas d'ordre<br/>global garanti"| KO["m1, m2, m3, m4...?<br/>ordre indéterminé"]
    end

    style TOPIC fill:#e3f2fd,stroke:#0d47a1
    style OK1 fill:#c8e6c9,color:#000
    style OK2 fill:#c8e6c9,color:#000
    style OK3 fill:#c8e6c9,color:#000
    style KO fill:#ffcdd2,color:#000,stroke:#c62828,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

#### Routage des messages vers les partitions

Chaque message doit être affecté à une partition précise. Le mécanisme de routage dépend de la présence ou non d'une **clé de message** (les messages Kafka sont des paires clé-valeur) :

**1. Message sans clé (clé `null`)**

Les messages sont distribués en **round-robin** entre les partitions : chaque nouveau message est assigné à la partition suivante dans la rotation. La charge est ainsi répartie uniformément. En revanche, **l'ordre n'est pas garanti** entre les messages d'un même producteur, car des messages successifs peuvent atterrir dans des partitions différentes.

> Exemple : les messages du thermostat 42, envoyés sans clé, sont dispersés sur plusieurs partitions — leur ordre de lecture ne reflète pas nécessairement leur ordre d'écriture.

##### Schéma — Distribution round-robin (sans clé)

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur<br/>(key = null)"]
        P -->|"m1"| P0["📦 Partition 0"]
        P -->|"m2"| P1["📦 Partition 1"]
        P -->|"m3"| P2["📦 Partition 2"]
        P -->|"m4"| P0
        P -->|"m5"| P1
        P -->|"m6"| P2

        P0 -.->|"contient"| C0["m1, m4, ..."]
        P1 -.->|"contient"| C1["m2, m5, ..."]
        P2 -.->|"contient"| C2["m3, m6, ..."]
    end

    style P fill:#43a047,color:#fff
    style P0 fill:#fff8e1,stroke:#f57c00
    style P1 fill:#fff8e1,stroke:#f57c00
    style P2 fill:#fff8e1,stroke:#f57c00
    style C0 fill:#e3f2fd,stroke:#1976d2
    style C1 fill:#e3f2fd,stroke:#1976d2
    style C2 fill:#e3f2fd,stroke:#1976d2
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**2. Message avec clé**

Lorsqu'un message possède une clé (ex. : un identifiant de capteur), Kafka applique une **fonction de hachage** sur cette clé, puis effectue un **modulo par le nombre de partitions** pour déterminer la partition cible :

```
partition = hash(clé) % nombre_de_partitions
```

Tous les messages partageant la même clé sont ainsi toujours écrits dans **la même partition**, ce qui garantit leur **ordre strict de traitement**. Des messages de clés différentes peuvent être assignés à n'importe quelle partition selon le résultat du hash.

##### Schéma — Routage par hash de la clé

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur"]
        P -->|"key=sensor-42"| H["🔢 hash(key)<br/>% nb_partitions"]
        P -->|"key=sensor-17"| H
        P -->|"key=sensor-42"| H
        P -->|"key=sensor-99"| H

        H -->|"→ 0"| P0["📦 Partition 0<br/>sensor-99"]
        H -->|"→ 1"| P1["📦 Partition 1<br/>sensor-42<br/>sensor-42"]
        H -->|"→ 2"| P2["📦 Partition 2<br/>sensor-17"]

        P1 -.->|"✅ même clé<br/>→ même partition<br/>→ ordre garanti"| ORDER["🎯 Ordre strict<br/>par clé"]
    end

    style P fill:#43a047,color:#fff
    style H fill:#7b1fa2,color:#fff,stroke:#4a148c,stroke-width:2px
    style P0 fill:#fff8e1,stroke:#f57c00
    style P1 fill:#fff8e1,stroke:#f57c00,stroke-width:3px
    style P2 fill:#fff8e1,stroke:#f57c00
    style ORDER fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

- **Traitement ordonné par entité** : utiliser l'identifiant d'un capteur, d'un utilisateur ou d'une transaction comme clé de message garantit que tous les événements liés à cette entité sont traités dans l'ordre, quelle que soit la vitesse de production.
- **Distribution de charge** : l'absence de clé permet une répartition homogène des messages sur toutes les partitions, idéale quand l'ordre n'a pas d'importance.

##### Schéma — Choisir entre ordre par entité et distribution uniforme

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        Q{"❓ Besoin métier"}
        Q -->|"ordre par entité<br/>(user, capteur, tx...)"| UC1["✅ Utiliser une clé<br/>= ID de l'entité"]
        Q -->|"répartition uniforme<br/>charge homogène"| UC2["✅ Pas de clé<br/>(round-robin)"]

        UC1 --> R1["🎯 Ordre garanti<br/>par entité"]
        UC2 --> R2["⚖️ Charge équilibrée<br/>sur le cluster"]
    end

    style Q fill:#fb8c00,color:#fff,stroke:#e65100,stroke-width:3px
    style UC1 fill:#43a047,color:#fff
    style UC2 fill:#1e88e5,color:#fff
    style R1 fill:#c8e6c9,stroke:#2e7d32
    style R2 fill:#bbdefb,stroke:#1565c0
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Comparaisons

| Critère | Sans clé (round-robin) | Avec clé (hash) |
|---|---|---|
| Distribution | Uniforme sur toutes les partitions | Concentrée sur une partition par clé |
| Ordre garanti | Non | Oui, pour les messages de même clé |
| Cas d'usage typique | Logs génériques, événements indépendants | Événements par entité (capteur, utilisateur…) |

##### Schéma — Comparaison visuelle round-robin vs hash

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction LR
        subgraph RR ["🔄 Round-robin (sans clé)"]
            direction TB
            RR0["📦 P0<br/>m1, m4, m7"]
            RR1["📦 P1<br/>m2, m5, m8"]
            RR2["📦 P2<br/>m3, m6, m9"]
            RRINFO["⚖️ Distribution uniforme<br/>❌ Pas d'ordre"]
        end

        subgraph HASH ["#️⃣ Hash de la clé"]
            direction TB
            H0["📦 P0<br/>(clés A, D)"]
            H1["📦 P1<br/>(clés B, E, F)"]
            H2["📦 P2<br/>(clé C)"]
            HINFO["🎯 Concentration par clé<br/>✅ Ordre par clé"]
        end
    end

    style RR fill:#e3f2fd,stroke:#0d47a1
    style HASH fill:#f3e5f5,stroke:#6a1b9a
    style RRINFO fill:#bbdefb,stroke:#1565c0
    style HINFO fill:#e1bee7,stroke:#6a1b9a
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Mises en garde

- Sans clé, **l'ordre des messages d'un même producteur n'est pas garanti**, même s'il existe une tendance générale (les messages plus anciens tendent à se trouver plus tôt dans une partition).
- Le partitionnement par clé garantit l'ordre **uniquement pour les messages partageant la même clé**. Des messages de clés différentes peuvent être entremêlés dans des ordres quelconques.
- Le nombre de partitions étant fixé à la création du topic (ou modifiable avec précaution), le résultat du hash peut changer si le nombre de partitions évolue, ce qui peut redistribuer les clés vers de nouvelles partitions.

##### Schéma — Effet d'une modification du nombre de partitions

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph BEFORE ["🟢 Avant : 3 partitions"]
            direction TB
            K1["key = sensor-42"] -->|"hash % 3 = 1"| BP1["📦 P1"]
        end

        subgraph AFTER ["🔴 Après : 5 partitions"]
            direction TB
            K2["key = sensor-42"] -->|"hash % 5 = 2"| AP2["📦 P2"]
        end

        BEFORE -.->|"⚠️ ajout de partitions"| AFTER

        AFTER -.->|"même clé,<br/>nouvelle partition !"| RISK["⚠️ Garantie d'ordre<br/>rompue à la transition"]
    end

    style BEFORE fill:#e8f5e9,stroke:#2e7d32
    style AFTER fill:#ffebee,stroke:#c62828
    style BP1 fill:#a5d6a7
    style AP2 fill:#ef9a9a
    style RISK fill:#ff5722,color:#fff,stroke:#bf360c,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Points à retenir

- Les **partitions** sont le mécanisme fondamental qui rend Kafka **scalable** : elles permettent à un topic de dépasser les limites d'un seul nœud.
- L'**ordre des messages est garanti uniquement au sein d'une partition**, pas à l'échelle du topic entier.
- **Sans clé** → distribution round-robin, pas d'ordre garanti.
- **Avec clé** → hash de la clé mod nombre de partitions → ordre garanti pour les messages de même clé.
- Kafka supporte jusqu'à ~**2 millions de partitions** par cluster (v4.0).

##### Schéma de synthèse — Carte mentale du module Partitions

<div align="center">

```mermaid
mindmap
  root((📦 Partitions<br/>Kafka))
    Définition
      Subdivision d'un topic
      Logs indépendants
      Réparties sur les brokers
    Pourquoi
      Scalabilité horizontale
      Dépasse limite d'1 nœud
      Stockage et débit accrus
    Échelle
      Centaines / milliers
      ~2M par cluster v4.0
    Routage
      Sans clé
        Round-robin
        Distribution uniforme
        Pas d'ordre
      Avec clé
        hash key mod N
        Même clé = même partition
        Ordre par clé
    Ordre
      Global non
      Par partition oui
      Par clé oui
    Pièges
      Pas d'ordre global
      Re-partitionnement
      Choix de la clé
```

</div>
