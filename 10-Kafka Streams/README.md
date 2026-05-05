# Stream Processing — Apache Kafka

## Table des matières
- [Module 1 : Stream Processing](#module-1--stream-processing)

---

## Module 1 : Stream Processing

### Sujet

Ce module introduit le **stream processing** dans l'écosystème Apache Kafka, explique pourquoi il est nécessaire d'adopter un framework dédié plutôt que d'enrichir les consumers, et présente les deux options principales : **Apache Flink** et **Kafka Streams**.

---

### Concepts clés

#### Pourquoi ne pas tout faire dans les consumers ?

```mermaid
graph LR
    A["Consumer basique<br/>poll() & iterate"]
    B["Consumer enrichi<br/>+ transformation"]
    C["Consumer complexe<br/>+ filtre + map"]
    D["Consumer très complexe<br/>+ aggrégation<br/>+ jointure<br/>+ fenêtres"]
    E["❌ IMPOSSIBLE<br/>sans framework<br/>de stream"]
    
    A -->|Simple| B
    B -->|Croissance| C
    C -->|Croissance| D
    D -->|Croissance| E
    
    Problems["PROBLÈMES:<br/>État persistant?<br/>Late arrivals?<br/>Out-of-order?<br/>Scalabilité?"]
    
    D -.->|Complexité| Problems
    
    style A fill:#c8e6c9
    style B fill:#fff9c4
    style C fill:#ffe0b2
    style D fill:#ffccbc
    style E fill:#ffcdd2
    style Problems fill:#ffcdd2
```

Dans une application Kafka en croissance, les consumers tendent à devenir de plus en plus complexes. Ce qui commence par de simples transformations stateless (masquage de données PII, reformatage de messages) évolue rapidement vers des opérations bien plus complexes :

- **Agrégations**
- **Enrichissements**
- **Jointures entre streams**
- **Traitement par fenêtres temporelles** (*time windows*)
- **Détection de patterns** (ex. : détection de fraude basée sur une séquence d'événements)
- **Gestion des messages en retard** (*late arriving messages*) et **hors ordre** (*out-of-order events*)

Or, l'API consumer de Kafka n'offre **aucun support natif** pour ces fonctionnalités avancées. Les implémenter soi-même reviendrait à écrire un framework entier, ce qui :
- N'est **pas le rôle du développeur applicatif** (son rôle est de délivrer de la valeur métier).
- Introduit des **problèmes de tolérance aux pannes** complexes (ex. : persistance de l'état en cas de crash).
- Est **extrêmement difficile à faire à grande échelle**.

La solution est d'adopter un **framework de stream processing dédié**.

---

### Les deux options principales

#### Apache Flink

Apache Flink est devenu le **standard de facto** pour le stream processing sur Kafka. Il fournit tous les primitives computationnels essentiels (transformation, filtrage, jointure, agrégation) sans nécessiter d'écrire du code d'infrastructure.

Flink propose **trois APIs** au choix :

```mermaid
graph TB
    Flink["Apache Flink<br/>Stream Processing Framework"]
    
    subgraph DS["DataStream API"]
        DS1["Bas niveau"]
        DS2["Historiquement première"]
        DS3["❌ Déconseillée<br/>pour nouveaux projets"]
        DS1 -.-> DS2 -.-> DS3
    end
    
    subgraph TA["Table API"]
        TA1["Haut niveau<br/>Java & Python"]
        TA2["Syntaxe fluente<br/>ressemble SQL"]
        TA3["✅ Recommandée<br/>pour nouveaux projets"]
        TA4["Croissance active"]
        TA1 -.-> TA2 -.-> TA3
        TA3 -.-> TA4
    end
    
    subgraph SQL["Flink SQL"]
        SQL1["SQL natif<br/>100% SQL standard"]
        SQL2["Parsing & Compilation"]
        SQL3["UDF support<br/>Python & Java"]
        SQL4["✅ Idéale<br/>pour tous les cas"]
        SQL1 -.-> SQL2 -.-> SQL3 -.-> SQL4
    end
    
    Flink --> DS
    Flink --> TA
    Flink --> SQL
    
    style DS fill:#ffcdd2
    style TA fill:#fff9c4
    style SQL fill:#c8e6c9
    style DS3 fill:#ff5252
    style TA3 fill:#66bb6a
    style SQL4 fill:#66bb6a
```

| API                | Niveau                      | Recommandation                                                                                                                                                                                 |
|--------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **DataStream API** | Bas niveau                  | Historiquement la première API de Flink. Toujours utilisée par certains experts, mais **déconseillée pour les nouveaux projets** car plus complexe à apprendre et moins activement développée. |
| **Table API**      | Haut niveau (Java / Python) | API fluente dont les méthodes ressemblent à des opérations SQL. Plus simple à comprendre, en croissance active. **Recommandée pour les nouveaux projets** en Java ou Python.                   |
| **Flink SQL**      | SQL natif                   | Permet d'écrire directement des requêtes SQL, qui sont parsées et transformées en jobs exécutés sur le cluster Flink. Offre toute l'expressivité de SQL pour le stream processing.             |

**Autres capacités notables de Flink :**
- Traitement de **données bornées en mode batch** (données statiques stockées en S3, etc.), avec le même code que pour le stream processing dans certains cas.
- Support des **User-Defined Functions (UDF)** appelables depuis Flink SQL, implémentables en Python ou Java.
- Détection de **doublons**.
- Adapté aux **cas d'usage à très grande échelle**.

#### Kafka Streams

Kafka Streams est une **librairie Java** incluse dans Apache Kafka open source. C'est un consumer Kafka enrichi qui intègre nativement les primitives de stream processing.

**Caractéristiques :**
- Fait partie d'**Apache Kafka open source** (pas de composant supplémentaire à déployer).
- Idéal pour les **équipes Java** qui déploient déjà des applications consommatrices Kafka.
- Dispose d'une bonne documentation, de tutoriels (dont sur Confluent Developer) et d'une communauté active.
- Bon **story de scalabilité**, mais tend à être privilégié pour des cas **small to medium scale**.

**Comparaison Flink vs Kafka Streams :**

```mermaid
graph TB
    subgraph Flink["Apache Flink<br/>Stream Processing Framework"]
        F1["Java, Python, SQL"]
        F2["Très grande échelle"]
        F3["Externe à Kafka"]
        F4["3 APIs riches"]
        F5["Cluster séparé<br/>ou managé"]
        F6["Batch native"]
    end
    
    subgraph KS["Kafka Streams<br/>Java Library"]
        KS1["Java only"]
        KS2["Small to medium<br/>scale"]
        KS3["Intégré dans Kafka"]
        KS4["1 API (Java)"]
        KS5["Embarqué<br/>dans app Java"]
        KS6["Stream only"]
    end
    
    subgraph Comparison["Quand utiliser?"]
        When1["✅ Flink:<br/>Grande échelle<br/>Multi-langage<br/>Batch + Stream"]
        When2["✅ Kafka Streams:<br/>Équipe Java<br/>Small/medium<br/>Déploiement simple"]
    end
    
    Flink --> Comparison
    KS --> Comparison
    
    style Flink fill:#e3f2fd
    style KS fill:#f3e5f5
    style Comparison fill:#e8f5e9
```

| Critère           | Apache Flink                     | Kafka Streams                       |
|-------------------|----------------------------------|-------------------------------------|
| Langage           | Java, Python, SQL                | Java                                |
| Échelle           | Très grande échelle              | Small à medium scale                |
| Intégration Kafka | Externe                          | Native (fait partie d'Apache Kafka) |
| APIs disponibles  | DataStream, Table, SQL           | API Streams (Java)                  |
| Déploiement       | Cluster Flink séparé (ou managé) | Embarqué dans l'application Java    |

Les deux options sont **valides et activement utilisées** en production.

---

### Fonctionnement — Exemple Flink SQL

L'exemple suivant illustre un pipeline de stream processing complet en Flink SQL sur des données de ratings de films en temps réel :

```mermaid
graph TB
    Raw["Topic Kafka: raw_ratings<br/>{movie_id, rating, timestamp}"]
    
    Window["🪟 Tumbling Window<br/>5 minutes<br/>GROUP BY movie_id<br/>AVG(rating)"]
    
    AvgRatings["Table: average_ratings<br/>{movie_id, avg_rating,<br/>window_start, window_end}"]
    
    MoviesRef["Topic Kafka: movies<br/>(Compacted)<br/>{movie_id, title}"]
    
    Join["🔗 JOIN<br/>average_ratings ⨝ movies<br/>ON movie_id"]
    
    Output["Table: rated_movies<br/>{title, avg_rating,<br/>window_time}<br/>OUTPUT to Topic"]
    
    Raw --> Window
    Window --> AvgRatings
    AvgRatings --> Join
    MoviesRef --> Join
    Join --> Output
    
    Capabilities["Capacités démontrées:<br/>✅ Fenêtres temporelles<br/>✅ Agrégations<br/>✅ Jointures entre streams<br/>✅ Tables de référence<br/>✅ Stateful processing<br/>✅ Sortie temps-réel"]
    
    Output -.-> Capabilities
    
    style Raw fill:#fff3e0
    style Window fill:#e3f2fd
    style AvgRatings fill:#e3f2fd
    style MoviesRef fill:#fff3e0
    style Join fill:#f3e5f5
    style Output fill:#e8f5e9
    style Capabilities fill:#e8f5e9
```

1. **Source** : Un topic Kafka `raw_ratings` contient des évaluations de films en streaming (movie ID, rating, timestamp).
2. **Table intermédiaire** : Une table `average_ratings` calcule la **moyenne des ratings par film** sur une **fenêtre glissante de 5 minutes** (*tumbling window*), groupée par `movie_id`.
3. **Table de référence** : Un topic Kafka `movies` (topic compacté contenant des entités plutôt que des événements) mappe les `movie_id` aux noms de films.
4. **Jointure** : Une jointure entre `average_ratings` et `movies` produit une table `rated_movies` avec les **titres de films et leurs notes moyennes** en temps réel.

Ce pipeline illustre plusieurs capacités clés : fenêtres temporelles, jointures entre streams, utilisation de topics compactés comme tables de référence.

---

### Opérations Stateless vs Stateful

```mermaid
graph TB
    subgraph Stateless["STATELESS<br/>Pas d'état persistant"]
        SL1["Map / Transform"]
        SL2["Filter"]
        SL3["Flatten"]
        SL4["✅ Simple<br/>✅ Rapide<br/>✅ Sans état"]
        
        SL1 -.-> SL4
        SL2 -.-> SL4
        SL3 -.-> SL4
    end
    
    subgraph Stateful["STATEFUL<br/>État persistant nécessaire"]
        SF1["Agrégation"]
        SF2["Jointure"]
        SF3["Fenêtres"]
        SF4["Pattern Detection"]
        SF5["⚠️ Complexe<br/>⚠️ État à gérer<br/>⚠️ Tolérance pannes"]
        
        SF1 -.-> SF5
        SF2 -.-> SF5
        SF3 -.-> SF5
        SF4 -.-> SF5
    end
    
    subgraph Solution["Solutions"]
        ManualCode["❌ Consumer seul<br/>= Code complexe<br/>= Fragile<br/>= Difficile à scale"]
        Framework["✅ Framework<br/>Flink/KStreams<br/>= Gestion native<br/>= Tolérance pannes<br/>= Scalable"]
    end
    
    Stateless --> Solution
    Stateful --> Solution
    
    ManualCode -.-> Framework
    
    style Stateless fill:#c8e6c9
    style Stateful fill:#ffccbc
    style Solution fill:#e3f2fd
    style ManualCode fill:#ffcdd2
    style Framework fill:#c8e6c9
```

---

### Cas d'usage

```mermaid
mindmap
  root((Stream Processing<br/>Cas d'usage))
    Détection Anomalies
      Fraude
      Anomalies système
      Patterns suspects
    Agrégations
      Moyennes temps-réel
      Comptages
      Sommes
      Fenêtres temporelles
    Enrichissement
      Jointure events + référence
      Data enrichment
      Context addition
    Nettoyage Data
      Dédoublonnage
      Déduplications
      Filtrage
    Transformations
      Reformatage
      Mapping
      Conversion format
    Batch Processing
      Flink only
      Datasets bornés
      S3, HDFS, etc
```

- **Détection de fraude** : corrélation de séquences d'événements pour identifier des patterns suspects.
- **Agrégations en temps réel** : calcul de moyennes, sommes, comptages sur des fenêtres de temps.
- **Enrichissement de données** : jointure d'un stream d'événements avec une table de référence.
- **Dédoublonnage** : détection et suppression de messages dupliqués dans un stream.
- **Reformatage et filtrage** : transformation légère de messages avant leur consommation.
- **Traitement batch** : avec Flink, traitement de datasets bornés (S3, etc.) avec le même code que le stream processing.

---

### Mises en garde

```mermaid
graph TB
    subgraph Dangers["⚠️ Pièges courants"]
        D1["Enrichir consumers<br/>avec logique stateful<br/>= Réécrire un framework<br/>= Coûteux & Fragile<br/>❌ Anti-pattern"]
        
        D2["Gérer état manuellement<br/>sans framework<br/>= Crash & perte données<br/>= Très difficile<br/>❌ Déprécié"]
        
        D3["Utiliser DataStream API<br/>Flink pour nouveaux projets<br/>= Complexe<br/>= Peu maintenu<br/>❌ Déconseillé"]
        
        D4["Kafka Streams<br/>pour équipes non-Java<br/>= Impossible<br/>= Dépend de Java<br/>❌ Non compatible"]
    end
    
    subgraph Solutions["✅ Solutions recommandées"]
        S1["Utiliser Flink<br/>ou Kafka Streams<br/>depuis le départ"]
        
        S2["Laisser framework<br/>gérer l'état<br/>persistance, pannes"]
        
        S3["Préférer Table API<br/>ou Flink SQL"]
        
        S4["Non-Java?<br/>Utiliser Flink<br/>(Java/Python/SQL)"]
    end
    
    D1 -.-> S1
    D2 -.-> S2
    D3 -.-> S3
    D4 -.-> S4
    
    style Dangers fill:#ffebee
    style Solutions fill:#e8f5e9
    style D1 fill:#ffcdd2
    style D2 fill:#ffcdd2
    style D3 fill:#ffcdd2
    style D4 fill:#ffcdd2
    style S1 fill:#c8e6c9
    style S2 fill:#c8e6c9
    style S3 fill:#c8e6c9
    style S4 fill:#c8e6c9
```

- **Ne pas enrichir indéfiniment les consumers** pour gérer de la logique stateful : cela revient à réécrire un framework de stream processing, ce qui est coûteux en temps et fragile.
- Les opérations **stateful** (agrégations, jointures) introduisent des problèmes de **tolérance aux pannes** (persistance de l'état en cas de crash) qui sont résolus nativement par Flink et Kafka Streams, mais très difficiles à gérer manuellement.
- La **DataStream API de Flink** est déconseillée pour les nouveaux projets : préférer la Table API ou Flink SQL.
- Kafka Streams est **limité au langage Java** : les équipes non-Java devront se tourner vers Flink.

---

### Points à retenir

```mermaid
graph TB
    SP["<b>Stream Processing</b><br/>Pilier essentiel Kafka"]
    
    subgraph Essential["Essentiels"]
        E1["✅ Incontournable<br/>au-delà de stateless"]
        E2["✅ Gère état natif<br/>avec tolérance pannes"]
        E3["✅ Scalable<br/>grande échelle"]
    end
    
    subgraph Flink["Apache Flink"]
        F1["Standard de facto"]
        F2["3 APIs:<br/>DataStream ❌<br/>Table API ✅<br/>Flink SQL ✅"]
        F3["Multi-langage:<br/>Java, Python, SQL"]
        F4["Batch + Stream"]
        F5["Grande échelle"]
    end
    
    subgraph KStreams["Kafka Streams"]
        K1["Librairie Java"]
        K2["Intégrée à Kafka<br/>open source"]
        K3["1 API fluente"]
        K4["Équipes Java"]
        K5["Small/medium scale"]
    end
    
    subgraph Choice["Quand choisir?"]
        Choice1["Flink:<br/>Complexité +++<br/>Échelle très grande<br/>Multi-langage"]
        Choice2["Kafka Streams:<br/>Complexité +<br/>Équipe Java<br/>Small/medium"]
        Choice3["Les deux sont<br/>viables<br/>en production"]
    end
    
    subgraph Cloud["Cloud"]
        Cloud1["Flink managé<br/>Confluent Cloud<br/>Zéro ops"]
    end
    
    SP --> Essential
    SP --> Flink
    SP --> KStreams
    Flink --> Choice
    KStreams --> Choice
    SP --> Cloud
    
    style SP fill:#e3f2fd
    style Essential fill:#c8e6c9
    style Flink fill:#fff9c4
    style KStreams fill:#f3e5f5
    style Choice fill:#e8f5e9
    style Cloud fill:#b2dfdb
```

- Le **stream processing est incontournable** dès que les opérations dépassent de simples transformations stateless.
- **Apache Flink** est le standard de facto pour le stream processing sur Kafka, avec trois APIs (DataStream, Table, SQL) et un support natif du mode batch.
- **Kafka Streams** est une librairie Java intégrée à Apache Kafka, idéale pour les équipes Java sur des charges small à medium scale.
- **Flink SQL** permet d'écrire des pipelines de stream processing en SQL pur, avec support des UDF, des fenêtres temporelles, des jointures et de la détection de doublons.
- Les deux frameworks (Flink et Kafka Streams) sont des **options viables et complémentaires** selon le contexte.
- Flink est disponible en **mode entièrement managé sur Confluent Cloud**, sans gestion de cluster.