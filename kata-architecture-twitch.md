# Exercice Kata d'architecture — Pipeline d'analyse Twitch en temps réel

## Contexte métier

Vous incarnez l'ensemble de l'équipe Data d'une entreprise spécialisée dans la fourniture de dashboards analytiques pour l'industrie du jeux vidéo. Un grand fournisseur de matériel informatique (périphériques, cartes graphiques) vous sollicite pour un projet de **veille en temps réel sur les streameurs Twitch**, en vue de qualifier des partenariats de placement produit.

Le client a deux besoins distincts :

1. **Performance des streameurs** : identifier les streameurs les plus suivis et suivre leur audience en temps réel pour établir un classement objectif.
2. **Brand safety** : analyser le contenu textuel des chats de ces streameurs pour s'assurer que leur communauté respecte les valeurs de la marque avant tout engagement contractuel.

---

## Étape 1 — Expression des besoins fonctionnels

À partir de l'énoncé client, l'équipe Data extrait les **KPIs** suivants :

| KPI | Type | Fenêtre temporelle      |
|---|---|-------------------------|
| Nombre de messages envoyés sur le chat | Comptage | Par tranche de 1 minute |
| Nombre de viewers au cours du stream | Mesure | Par tranche de 1 minute |
| Nombre maximum de viewers atteint | Agrégat global | Session complète        |
| Nombre d'utilisateurs uniques ayant interagi | Cardinalité | Par session             |
| *(Optionnel)* Nombre de messages offensants | Comptage filtré | Par tranche de 1 minute |

**Questions à se poser en équipe :**

- Pour cette session nous définissons un utilisateur unique par son pseudo donc un pseudo = un utilisateur
- Le client exprime le besoin de stocker l'historique des statistiques calculées.
- [Optionnel] Le client fournit une liste de mots-clés pour définir un message "offensant".

---

## Étape 2 — Identification des exigences non fonctionnelles

| Exigence | Niveau attendu | Justification |
|---|---|---|
| **Latence** | < 30 secondes | Le client veut un dashboard "en temps réel" — les métriques doivent être fraîches |
| **Volume** | Élevé | Un top streameur peut générer plusieurs milliers de messages/minute |
| **Scalabilité** | Horizontale | Le nombre de streameurs à surveiller peut augmenter sans refonte |
| **Disponibilité** | Haute (99,9 %) | Le service est vendu à un client B2B avec SLA |
| **Sécurité** | Moyenne-haute | Les données de chat sont publiques, mais les credentials API Twitch doivent être protégés |
| **Conformité RGPD** | Obligatoire | Les pseudonymes Twitch sont des données à caractère personnel (UE) |
| **Maintenabilité** | Haute | L'équipe doit pouvoir ajouter de nouveaux KPIs sans réécrire l'architecture |

---

## Étape 3 — Choix du style architectural

Deux styles d'architecture sont envisageables :

**Architecture Kappa** ou **Architecture Lambda** ?

---

## Étape 4 — Schéma d'architecture cible

Complétez le schéma d'architecture ci-dessous en fonction des données issues des étapes précédentes.

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                        │
│  De quels données avons nous besoin ?                            │
│  Quelles APIs pouvons-nous utiliser ?                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE D'INGESTION                           │
│  Quelles technologies pouvons-nous utiliser ?                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE DE TRAITEMENT                         │
│  Quel framework / technonolies devons nous utiliser ?            │
│  La charge dois-elle être distribuée ? Sera-t-elle variable ?    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                               │
│  Le but étant de créer un dashboard pour l'utilisateur, quelles  │
│  technologie allons nous utiliser pour consommer et afficher les │
│  données sous form de dashboard ?                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     COUCHE D'EXPOSITION                          │
│  Dashboard temps réel                                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Étape 5 — Rôles et responsabilités de l'équipe

Complétez le tableau suivant en définissant quelles actions sont menée par quels rôles.

| Rôle | Responsabilité sur ce projet |
|---|------------------------------|
| **Data Architect** | ?                            |
| **Data Engineer** | ?                            |
| **Data Scientist** | ?                            |
| **Data Analyst** | ?                            |
| **Data Manager** | ?                            |

---

## Points de vigilance RGPD

Les pseudonymes Twitch, bien que publics, sont des **données à caractère personnel** au sens du RGPD.
Comment faire en sorte que le système respecte le RGPD ?