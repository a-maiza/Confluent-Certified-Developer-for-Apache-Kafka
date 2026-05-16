# Comprendre les ACLs et l’Authorization dans Kafka

## 1. Qu’est-ce qu’une ACL dans Kafka ?

une ACL (Access Control List) permet de contrôler les permissions d’accès aux ressources Kafka.

Les ACLs répondent à des questions comme :

- Qui peut lire un topic ?
- Qui peut écrire dans un topic ?
- Qui peut créer ou supprimer des topics ?
- Quel consumer peut rejoindre un consumer group ?

Kafka utilise donc un système de sécurité basé sur :
- l’authentification (Authentication),
- puis l’autorisation (Authorization via ACLs).

---

# 2. Différence entre Authentication et Authorization

## Authentication = “Qui es-tu ?”

L’authentification sert à identifier le client qui se connecte à Kafka.

Kafka vérifie :
- username/password,
- certificat SSL,
- token,
- Kerberos,
- etc.

Exemple :

```text
User:payment-app
```

Kafka sait alors :
> “Le client connecté est payment-app”

### Mécanismes d’authentification courants

- SASL/PLAIN
- SASL/SCRAM
- SSL/TLS
- Kerberos
- OAuth

---

## Authorization = “Que peux-tu faire ?”

Une fois l’identité connue, Kafka vérifie les permissions.

Exemple :

```text
Allow User:payment-app Write Topic:payments
```

Cela signifie :
- `payment-app`
- peut écrire (`Write`)
- dans le topic `payments`

---

# Résumé simple

| Concept        | Question            |
|----------------|---------------------|
| Authentication | Qui es-tu ?         |
| Authorization  | Que peux-tu faire ? |

---

# 3. Fonctionnement interne des ACLs dans Kafka

Quand un producer ou consumer envoie une requête :

```text
Producer/Consumer
        |
        v
+-------------------+
| Authentication    |
+-------------------+
        |
        v
+-------------------+
| Authorization     |
| (ACL Check)       |
+-------------------+
        |
   Allow / Deny
        |
        v
+-------------------+
| Kafka Operation   |
+-------------------+
```

---

# Étape 1 — Connexion au broker

Le client ouvre une connexion réseau vers Kafka.

Exemple :

```properties
listeners=SASL_SSL://:9092
```

---

# Étape 2 — Authentication

Le client s’authentifie.

Exemple avec SCRAM :

```text
User:payment-app
```

Kafka associe alors cette identité au client.

---

# Étape 3 — Requête Kafka

Le client demande une opération.

Exemple :
- produire un message,
- lire un topic,
- rejoindre un consumer group.

Supposons :

```text
User:payment-app -> Write -> Topic:payments
```

---

# Étape 4 — Construction du contexte d’autorisation

Kafka construit un contexte contenant :

| Élément   | Exemple          |
|-----------|------------------|
| Principal | User:payment-app |
| Operation | Write            |
| Resource  | Topic:payments   |
| Host      | IP du client     |

---

# Étape 5 — Appel de l’Authorizer

Kafka appelle le moteur d’autorisation :

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
```

L’Authorizer :
- charge les ACLs,
- cherche les permissions,
- décide Allow/Deny.

---

# Étape 6 — Vérification des ACLs

Kafka cherche une ACL correspondante :

```text
Allow User:payment-app Write Topic:payments
```

---

# Étape 7 — Décision finale

Kafka applique les règles :

## 1. Deny prioritaire

Si une ACL `Deny` existe :

```text
Deny User:payment-app Write Topic:payments
```

=> accès refusé immédiatement.

---

## 2. Sinon recherche d’un Allow

```text
Allow User:payment-app Write Topic:payments
```

=> accès autorisé.

---

## 3. Sinon

Kafka applique :

```text
Default = Deny
```

Donc sans ACL explicite :
=> accès refusé.

---

# Étape 8 — Résultat

## Si autorisé

Kafka exécute l’opération.

## Sinon

Kafka retourne une erreur :

```text
TopicAuthorizationException
```

ou :

```text
GroupAuthorizationException
```

---

# 4. Où sont stockées les ACLs ?

## Ancien Kafka (ZooKeeper)

Les ACLs étaient stockées dans ZooKeeper.

---

## Kafka moderne (KRaft)

Les ACLs sont stockées :
- directement dans Kafka,
- dans le metadata log.

Elles sont répliquées entre les controllers Kafka.

---

# 5. Qui peut créer les ACLs ?

Seuls :
- les administrateurs Kafka,
- les super users,
- ou les utilisateurs ayant les permissions d’administration.

En pratique on utilise :

```bash
kafka-acls.sh
```

Exemple :

```bash
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:app1 \
  --operation Read \
  --topic orders
```

---

# 6. Pourquoi les producers/consumers ne créent pas leurs ACLs ?

Parce que ce serait dangereux.

Un consumer ne doit pas pouvoir dire :

```text
"Je me donne les droits admin"
```

Les applications :
- fournissent leur identité,
- mais ne décident jamais des permissions.

Kafka centralise toute la sécurité côté broker.

---

# 7. Où configure-t-on les ACLs ?

## Les ACLs sont activées uniquement côté broker Kafka

Exemple :

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
```

Cette configuration active :
- le moteur d’autorisation,
- dans le broker Kafka.

---

# 8. Peut-on activer les ACLs côté Producer/Consumer ?

Non.

Les producers/consumers :
- ne gèrent pas l’autorisation,
- ils ne font que s’authentifier.

Ils envoient simplement :

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
```

et :

```properties
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
username="app1" \
password="secret";
```

Le client dit seulement :

```text
"Je suis User:app1"
```

Kafka décide ensuite :
- ce qu’il peut faire,
- via les ACLs.

---

# 9. Configuration minimale pour activer les ACLs

## Activation de l’Authorizer

```properties
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
```

---

## Définition des super users

```properties
super.users=User:admin;User:kafka
```

Les super users bypassent toutes les ACLs.

---

## Activation SASL

```properties
listeners=SASL_PLAINTEXT://:9092
advertised.listeners=SASL_PLAINTEXT://localhost:9092
```

---

## Configuration SCRAM

```properties
sasl.enabled.mechanisms=SCRAM-SHA-256
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
security.inter.broker.protocol=SASL_PLAINTEXT
```

---

# 10. Ajouter une ACL

Exemple :

```bash
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:app1 \
  --operation Read \
  --topic orders
```

---

# 11. Exemple complet de flow

## Étape 1 — Producer démarre

```text
Producer -> Kafka
```

---

## Étape 2 — Authentication

```text
User:payment-app
```

---

## Étape 3 — Kafka reçoit :

```text
Write -> Topic:payments
```

---

## Étape 4 — Kafka cherche une ACL :

```text
Allow User:payment-app Write Topic:payments
```

---

## Étape 5 — Résultat

### ACL trouvée

```text
Authorized
```

### ACL absente

```text
TopicAuthorizationException
```

---

# 12. Ressources Kafka protégées par ACLs

| Resource Type   | Exemple         |
|-----------------|-----------------|
| Topic           | payments        |
| Group           | analytics-group |
| Cluster         | kafka-cluster   |
| TransactionalId | tx-app          |
| DelegationToken | token           |

---

# 13. Opérations possibles dans les ACLs

| Opération     | Description     |
|---------------|-----------------|
| Read          | Lire            |
| Write         | Écrire          |
| Create        | Créer           |
| Delete        | Supprimer       |
| Alter         | Modifier        |
| Describe      | Voir metadata   |
| ClusterAction | Actions cluster |

---

# 14. Point très important

Kafka applique :

```text
Default = Deny
```

Donc :
- si aucune ACL n’existe,
- l’accès est refusé.

---

# 15. Résumé mental ultra simple

Le producer/consumer dit :

```text
"Voici mon identité"
```

Kafka répond :

```text
"Très bien. Maintenant je vérifie ce que tu as le droit de faire."
```