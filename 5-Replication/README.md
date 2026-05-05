# Apache Kafka — Replication

## Table des matières
- [Module 1 : Replication dans Apache Kafka](#module-1--replication-dans-apache-kafka)

---

## Module 1 : Replication dans Apache Kafka

### Sujet

Ce module introduit le mécanisme de **replication** dans Apache Kafka : pourquoi il est indispensable, comment il fonctionne, et ce qu'il apporte en termes de fiabilité et de tolérance aux pannes.

---

### Concepts clés

**Replication Factor**
Le *replication factor* est un paramètre de configuration qui définit le nombre de copies de chaque partition. Avec un *replication factor* de 3, chaque partition est dupliquée sur 3 brokers différents. Ce paramètre est critique : il détermine directement le niveau de tolérance aux pannes du cluster.

##### Schéma — Replication factor = 3 (1 partition dupliquée 3 fois)

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        T["📋 Topic — Partition P0<br/>replication.factor = 3"]
        T --> B1["🖥️ Broker 1<br/>📦 P0 (copie 1)"]
        T --> B2["🖥️ Broker 2<br/>📦 P0 (copie 2)"]
        T --> B3["🖥️ Broker 3<br/>📦 P0 (copie 3)"]
        B1 -.->|"objectif"| FT["🛡️ Tolérance aux pannes<br/>= 2 brokers peuvent tomber"]
        B2 -.-> FT
        B3 -.-> FT
    end

    style T fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style B1 fill:#fff8e1,stroke:#f57c00
    style B2 fill:#fff8e1,stroke:#f57c00
    style B3 fill:#fff8e1,stroke:#f57c00
    style FT fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Leader Replica**
Chaque partition possède une *leader replica* (réplica leader). C'est sur ce leader que toutes les écritures et, par défaut, toutes les lectures sont effectuées. Il n'y a toujours qu'un seul leader par partition à un instant donné.

##### Schéma — Le leader, point d'entrée unique des écritures

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        P["🏭 Producteur"] -->|"✍️ écriture"| L["👑 Leader Replica<br/>(unique)"]
        C["📱 Consommateur"] -->|"📖 lecture (défaut)"| L
        L -.->|"1 seul leader<br/>par partition<br/>à un instant T"| RULE["📜 Règle"]
    end

    style P fill:#43a047,color:#fff
    style C fill:#fb8c00,color:#fff
    style L fill:#e53935,color:#fff,stroke:#b71c1c,stroke-width:3px
    style RULE fill:#fff9c4,stroke:#f57f17
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

**Follower Replicas**
Les *follower replicas* sont les copies passives de la partition. Pour un *replication factor* de `n`, il y a `n - 1` followers. Leur rôle est de répliquer en continu les messages écrits dans le leader, aussi vite que possible, afin de rester synchronisés.

##### Schéma — Leader & followers (replication factor = n)

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        L["👑 Leader<br/>(1)"]
        L -->|"réplication continue"| F1["🔁 Follower 1"]
        L -->|"réplication continue"| F2["🔁 Follower 2"]
        L -->|"réplication continue"| FN["🔁 Follower n-1"]

        L -.->|"replication.factor = n<br/>→ 1 leader + (n-1) followers"| INFO["ℹ️ Total = n copies"]
    end

    style L fill:#e53935,color:#fff,stroke:#b71c1c,stroke-width:3px
    style F1 fill:#fff8e1,stroke:#f57c00
    style F2 fill:#fff8e1,stroke:#f57c00
    style FN fill:#f5f5f5,stroke:#9e9e9e,stroke-dasharray:3 3
    style INFO fill:#e3f2fd,stroke:#1565c0
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Fonctionnement

1. **Écriture** : Toute écriture de données passe obligatoirement par le leader de la partition concernée, sans exception.
2. **Réplication** : Les followers surveillent le leader en permanence et *scraping* (récupèrent) les nouveaux messages au fur et à mesure qu'ils arrivent, dans le but de maintenir une copie à jour.
3. **Lecture** : Par défaut, les lectures se font également sur le leader. Depuis les versions récentes de Kafka, il est possible de configurer le client pour qu'il lise depuis le *replica* le plus proche géographiquement ou réseau, y compris un follower, afin de réduire la latence.

##### Schéma — Cycle écriture / réplication / lecture

<div align="center">

```mermaid
sequenceDiagram
    autonumber
    participant P as 🏭 Producteur
    participant L as 👑 Leader
    participant F1 as 🔁 Follower 1
    participant F2 as 🔁 Follower 2
    participant C as 📱 Consommateur

    rect rgb(250, 250, 250)
        Note over P,L: 1️⃣ Écriture
        P->>L: write(message)
        L-->>P: ack

        Note over L,F2: 2️⃣ Réplication
        F1->>L: fetch(offset)
        L-->>F1: message(s)
        F2->>L: fetch(offset)
        L-->>F2: message(s)

        Note over L,C: 3️⃣ Lecture (défaut)
        C->>L: fetch(offset)
        L-->>C: message(s)
    end
```

</div>

---

##### Schéma — Lecture depuis le replica le plus proche (option latence)

<div align="center">

```mermaid
flowchart LR
    subgraph FRAME [" "]
        direction LR
        subgraph DEFAULT ["📍 Lecture par défaut"]
            CD["📱 Consommateur<br/>(zone B)"] -->|"latence ↑"| LD["👑 Leader<br/>(zone A)"]
        end

        subgraph NEAR ["📍 Lecture replica le plus proche"]
            CN["📱 Consommateur<br/>(zone B)"] -->|"latence ↓"| FN["🔁 Follower<br/>(zone B)"]
            FN -.->|"réplique depuis"| LN["👑 Leader<br/>(zone A)"]
        end
    end

    style DEFAULT fill:#fff3e0,stroke:#e65100
    style NEAR fill:#e8f5e9,stroke:#2e7d32
    style LD fill:#e53935,color:#fff
    style LN fill:#e53935,color:#fff
    style FN fill:#fff8e1,stroke:#f57c00,stroke-width:3px
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Tolérance aux pannes

En cas de défaillance d'un broker :
- Les partitions dont le leader résidait sur ce broker deviennent indisponibles temporairement.
- Le cluster **élit automatiquement un nouveau leader** parmi les followers encore disponibles pour chacune de ces partitions.
- Les données ne sont pas perdues tant qu'au moins un *replica* est intact.
- Une fois le broker défaillant restauré (ou remplacé), le cluster peut reconstituer les replicas manquants pour revenir au *replication factor* cible.

**Important** : la réélection du leader est automatique et transparente pour les applications clientes — le cluster reste disponible et les données restent accessibles.

##### Schéma — Failover automatique d'un leader

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        subgraph T0 ["🟢 t0 — État sain"]
            direction LR
            B1A["🖥️ Broker 1<br/>👑 Leader P0"]
            B2A["🖥️ Broker 2<br/>🔁 Follower P0"]
            B3A["🖥️ Broker 3<br/>🔁 Follower P0"]
        end

        subgraph T1 ["🔴 t1 — Broker 1 tombe"]
            direction LR
            B1B["🖥️ Broker 1<br/>💥 KO"]
            B2B["🖥️ Broker 2<br/>🔁 Follower P0"]
            B3B["🖥️ Broker 3<br/>🔁 Follower P0"]
            B1B -.->|"P0 indisponible<br/>brièvement"| TEMP["⏱️ Temporaire"]
        end

        subgraph T2 ["🟢 t2 — Nouveau leader élu"]
            direction LR
            B1C["🖥️ Broker 1<br/>💥 KO"]
            B2C["🖥️ Broker 2<br/>👑 Leader P0 (nouveau)"]
            B3C["🖥️ Broker 3<br/>🔁 Follower P0"]
            B2C -.->|"transparent<br/>pour les clients"| OK["✅ Service rétabli"]
        end

        T0 --> T1 --> T2

        T2 -.->|"après réparation"| RESTORE["🔧 Reconstitution<br/>des replicas manquants"]
    end

    style T0 fill:#e8f5e9,stroke:#2e7d32
    style T1 fill:#ffebee,stroke:#c62828
    style T2 fill:#e8f5e9,stroke:#2e7d32
    style B1A fill:#ef5350,color:#fff
    style B1B fill:#9e9e9e,color:#fff,stroke-dasharray:3 3
    style B1C fill:#9e9e9e,color:#fff,stroke-dasharray:3 3
    style B2C fill:#ef5350,color:#fff,stroke:#b71c1c,stroke-width:3px
    style TEMP fill:#fff9c4
    style OK fill:#c8e6c9
    style RESTORE fill:#bbdefb,stroke:#1565c0
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Cas d'usage

- **Fiabilité en auto-hébergement** : si vous gérez vous-même les serveurs Kafka (bare metal ou instances cloud), la réplication est un point absolument critique à configurer et surveiller.
- **Services cloud managés** : si vous utilisez un service Kafka cloud (ex. Confluent Cloud, Amazon MSK), la réplication est gérée automatiquement en arrière-plan — vous n'avez pas à vous en préoccuper opérationnellement.
- **Optimisation de performance** : dans les environnements où la latence réseau est un enjeu, il est possible de configurer les clients lecteurs pour consommer depuis le *replica* le plus proche, au lieu du leader, afin de gagner en rapidité.

##### Schéma — Trois cas d'usage de la replication

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        REPL["🛡️ Replication"]
        REPL --> UC1["🛠️ Auto-hébergement<br/>→ critique à configurer<br/>+ surveiller"]
        REPL --> UC2["☁️ Cloud managé<br/>→ géré automatiquement<br/>(transparent)"]
        REPL --> UC3["⚡ Optimisation latence<br/>→ lire depuis le replica<br/>le plus proche"]
    end

    style REPL fill:#1e88e5,color:#fff,stroke:#0d47a1,stroke-width:3px
    style UC1 fill:#fff3e0,stroke:#e65100
    style UC2 fill:#e3f2fd,stroke:#1565c0
    style UC3 fill:#e8f5e9,stroke:#2e7d32
    style FRAME fill:#fafafa,stroke:#90caf9,stroke-width:2px,stroke-dasharray:5 5
```

</div>

---

### Mises en garde

- Stocker une partition sur un seul broker est insuffisant : les disques et serveurs peuvent tomber en panne à tout moment.
- Après une défaillance, même si le cluster élit un nouveau leader automatiquement, il reste nécessaire de **restaurer le nombre de replicas** pour retrouver le niveau de résilience souhaité.
- Lire depuis un follower (plutôt que le leader) peut introduire un léger décalage si le follower n'est pas complètement synchronisé. Cette option est à réserver aux cas où la performance prime sur la cohérence stricte.

##### Schéma — Pièges à éviter

<div align="center">

```mermaid
flowchart TB
    subgraph FRAME [" "]
        direction TB
        WARN["⚠️ Mises en garde"]
        WARN --> W1["❌ replication.factor = 1<br/><i>1 panne disque = perte de données</i>"]
        WARN --> W2["❌ Oublier de restaurer<br/>les replicas après panne<br/><i>résilience dégradée</i>"]
        WARN --> W3["❌ Lire depuis un follower<br/>quand cohérence stricte requise<br/><i>léger lag possible</i>"]
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

- La **replication** est un mécanisme fondamental de la fiabilité de Kafka.
- Elle assure **fault tolerance** (tolérance aux pannes), **load balancing**, et des performances saines à l'échelle du cluster.
- Les **écritures** vont toujours vers le **leader**.
- Les **lectures** vont par défaut vers le **leader**, mais peuvent être dirigées vers le **replica le plus proche** pour optimiser la latence.
- En cas de panne, Kafka élit automatiquement un nouveau leader — **aucune perte de données** tant qu'un replica est disponible.
- C'est une fonctionnalité **entièrement intégrée** à Kafka, sans dépendance externe.

##### Schéma de synthèse — Carte mentale du module Replication

<div align="center">

```mermaid
mindmap
  root((🛡️ Replication<br/>Kafka))
    Replication factor
      Nombre de copies
      Ex RF = 3
      Tolérance aux pannes
    Roles
      Leader replica
        Unique
        Toutes les écritures
        Lectures par défaut
      Follower replicas
        n moins 1 followers
        Réplication continue
        Synchronisation
    Cycle
      Écriture vers leader
      Réplication vers followers
      Lecture leader ou plus proche
    Failover
      Détection panne
      Élection nouveau leader
      Transparent clients
      Pas de perte si 1 replica OK
    Cas usage
      Auto-hébergement critique
      Cloud managé transparent
      Optimisation latence
    Pièges
      RF = 1 dangereux
      Restaurer les replicas
      Lecture follower lag
```

</div>
