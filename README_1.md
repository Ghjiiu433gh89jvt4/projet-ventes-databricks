# Projet Ventes — Pipeline de données de bout en bout

Pipeline de données complet simulant un environnement d'entreprise : ingestion, gouvernance, transformation et restitution analytique, construit sur la stack **Azure Data Factory · Azure Data Lake Storage Gen2 · Databricks (Unity Catalog) · Power BI**.

Projet réalisé dans un objectif d'apprentissage pratique et de préparation à des postes de Data Engineer, en appliquant les standards observés en entreprise (architecture médaillon, gouvernance des données, orchestration, contrôle qualité, sécurisation des secrets).

---

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────────────────────────────┐      ┌──────────┐
│   raw       │      │     ADF      │      │              Databricks              │      │ Power BI │
│  (ADLS)     │─────▶│   Pipeline   │─────▶│         Workflow (6 tâches)          │─────▶│          │
│             │ Copy │  paramétré   │Trigger│                                       │Connecteur│          │
│ ventes.csv  │      │  + contrôle  │ API   │  bronze → silver → gold              │Databricks│          │
│ retours.csv │      │   qualité    │       │  (Unity Catalog, tables Delta)       │          │          │
└─────────────┘      └──────────────┘      └─────────────────────────────────────┘      └──────────┘
```

**Flux de données complet :**

```
raw/ventes/ventes.csv ──┐
raw/retours/retours.csv ┴──▶ [ADF: PL_Ingestion_Raw_To_Bronze] ──▶ bronze/{table}/ingestion_date=.../
                                                                              │
                                                                              ▼ (déclenchement automatique)
                                                          [Databricks Workflow: Pipeline_Ventes_Silver_Gold]
                                                                              │
                        bronze.ventes.{ventes,retours}  (tables Delta, Unity Catalog)
                                       │
                        silver.ventes.{ventes_clean, ventes_rejected, retours_clean, retours_rejected}
                                       │
                        gold.ventes.{dim_produit, dim_region, dim_client, dim_vendeur, dim_temps,
                                      fact_ventes, fact_retours, kpi_ventes_mensuel}
                                       │
                                       ▼
                                  Power BI (Import, connecteur Databricks)
```

---

## Stack technique

| Composant | Rôle dans le projet |
|---|---|
| **Azure Data Lake Storage Gen2** | Stockage physique, hiérarchie de containers `raw` / `bronze` / `silver` / `gold` |
| **Azure Data Factory** | Orchestration de l'ingestion raw → bronze, déclenchement du traitement Databricks |
| **Databricks (Serverless + Unity Catalog)** | Transformation, gouvernance, modélisation en étoile |
| **Databricks Workflows** | Orchestration bronze → silver → gold (DAG de dépendances) |
| **Azure Key Vault** | Stockage sécurisé du token d'authentification inter-services |
| **Power BI** | Restitution et analyse, connecté à la couche gold |
| **Git / GitHub** | Versioning du code (notebooks, pipelines documentés) |

---

## Structure du repository

```
Projet_Ventes/
├── 00_setup/
│   └── init_catalogs_schemas      # Création des catalogues et schémas Unity Catalog (idempotent)
│
├── 01_bronze/
│   └── bronze_ingest               # CSV brut → table Delta gouvernée, avec traçabilité
│
├── 02_silver/
│   ├── silver_ventes               # Nettoyage, cast de dates, filtrage, quarantaine
│   └── silver_retours               # Idem + détection des retours orphelins
│
├── 03_gold/
│   ├── gold_dimensions             # Construction des tables de dimensions (modèle en étoile)
│   ├── gold_facts                  # Construction des tables de faits, jointes aux dimensions
│   └── gold_aggregations           # KPI pré-calculés pour Power BI (table physique)
│
├── libs/
│   ├── schema_definitions          # Schémas Spark explicites (StructType), par table source
│   └── data_quality                # Fonctions réutilisables de nettoyage / quarantaine
│
└── exploration/                    # Bac à sable, jamais exécuté en production
```

**Note sur le déploiement :** le code est développé et commité depuis un Git folder personnel (`Users/`), puis promu vers un Git folder `Shared/` via un `git pull` volontaire — ce second dossier est celui que les Jobs Databricks exécutent réellement, ce qui sépare le merge du déploiement.

---

## Détail des composants

### Pipeline ADF — `PL_Ingestion_Raw_To_Bronze`

Pipeline paramétré traitant plusieurs sources sans duplication de logique.

| Activité | Rôle |
|---|---|
| `Set Ingestion Date` | Fixe une variable `v_ingestion_date` (format `yyyyMMdd`), utilisée pour partitionner bronze |
| `ForEach Source` | Boucle sur le paramètre `p_sources` (`["ventes", "retours"]`) — un seul pipeline pour toutes les sources |
| `Check Raw Folder` (Get Metadata) | Vérifie l'existence de fichiers dans `raw/{source}/` avant de copier |
| `If Files Exist` | Aiguille vers la copie si des fichiers sont présents, vers un échec explicite sinon |
| `Copy to Bronze` | Copie `raw/{source}/*.csv` vers `bronze/{source}/ingestion_date={date}/` |
| `Fail - No Source File` | Échec volontaire et explicite si le dossier source est vide (évite un succès silencieux trompeur) |
| `Get Databricks Token` | Récupère le token d'authentification depuis Azure Key Vault |
| `Store Token` | Stocke le token dans une variable de pipeline |
| `Trigger Databricks Workflow` | Appelle l'API REST Databricks (`jobs/run-now`) pour déclencher le traitement bronze→gold |

**Datasets paramétrés :** `DS_Raw_Generic` et `DS_Bronze_Generic`, chacun avec les paramètres `p_container` / `p_folder` (et `p_ingestion_date` pour le sink) — un seul dataset de chaque type sert toutes les sources.

### Notebooks Databricks

| Notebook | Entrée | Sortie | Rôle |
|---|---|---|---|
| `init_catalogs_schemas` | — | Catalogues `bronze`/`silver`/`gold`, schéma `ventes` | Initialisation Unity Catalog (idempotent, `IF NOT EXISTS`) |
| `bronze_ingest` | CSV brut (dernier `ingestion_date`) | `bronze.ventes.{ventes,retours}` | Lecture typée (schéma explicite), ajout de traçabilité (`_ingestion_date`, `_ingested_at`, `_source_file`), écriture idempotente par partition (`replaceWhere`) |
| `silver_ventes` | `bronze.ventes.ventes` | `silver.ventes.ventes_clean` + `ventes_rejected` | Standardisation de casse, cast sécurisé des dates, filtrage des quantités invalides, détection des champs critiques manquants, dédoublonnage — chaque rejet tagué par motif |
| `silver_retours` | `bronze.ventes.retours` + `silver.ventes.ventes_clean` | `silver.ventes.retours_clean` + `retours_rejected` | Idem + détection des retours orphelins (comparaison contre silver, pas bronze) et incohérences temporelles |
| `gold_dimensions` | `silver.ventes.*` | `dim_produit`, `dim_region`, `dim_client`, `dim_vendeur`, `dim_temps` | Extraction des valeurs distinctes, génération de clés de substitution (surrogate keys) ; `dim_temps` générée de façon exhaustive sur 5 ans |
| `gold_facts` | `silver.ventes.*` + dimensions | `fact_ventes`, `fact_retours` | Remplacement des attributs texte par les clés techniques des dimensions, avec contrôle explicite des valeurs nulles post-jointure |
| `gold_aggregations` | `fact_ventes`, `fact_retours` + dimensions | `kpi_ventes_mensuel` | Agrégation CA / retours par mois-région-catégorie, avec séparation des agrégats avant jointure (évite le fan-out) |

### Fonctions réutilisables (`libs/`)

- **`schema_definitions`** : schémas Spark explicites par table source, évite `inferSchema` (lenteur, instabilité)
- **`data_quality`** : `standardize_text_column`, `safe_cast_date`, `filter_invalid_quantities`, `remove_exact_duplicates`, `flag_orphan_records`, `log_quality_summary` — chaque fonction de filtrage retourne `(valide, rejeté)` plutôt que de supprimer silencieusement

---

## Orchestration Databricks — `Pipeline_Ventes_Silver_Gold`

Job Databricks (Workflows) enchaînant les 6 notebooks avec dépendances explicites (DAG) :

```
bronze_ingest → silver_ventes → silver_retours → gold_dimensions → gold_facts → gold_aggregations
```

Chaque tâche pointe vers le Git folder `Shared/` (source = Workspace), garantissant que seule une version explicitement déployée est exécutée. Compute : Serverless sur toutes les tâches.

---

## Modèle de données (gold)

Modèle en étoile classique :

- **Table de faits** : `fact_ventes` (grain = une vente), `fact_retours` (grain = un retour)
- **Dimensions** : `dim_produit`, `dim_region`, `dim_client`, `dim_vendeur`, `dim_temps`
- **Table pré-agrégée** : `kpi_ventes_mensuel` (CA, quantités, taux de retour par mois/région/catégorie)

Chaque dimension porte une clé de substitution (surrogate key) indépendante de la clé métier source, pour la stabilité du modèle et la performance des jointures.

---

## Qualité des données

Principe appliqué à chaque étape silver : **quarantaine plutôt que suppression silencieuse**. Toute ligne rejetée (date invalide, quantité négative, champ critique manquant, référence orpheline) est conservée dans une table `*_rejected` avec un motif explicite, permettant l'audit et la correction à la source sans perte de traçabilité.

---

## Sécurité

- Authentification ADLS ↔ Databricks via **Access Connector** (identité managée), sans clé de compte de stockage
- Authentification ADF ↔ ADLS via identité managée + rôle RBAC `Storage Blob Data Contributor`
- Token d'authentification ADF → Databricks stocké dans **Azure Key Vault**, jamais en clair dans le pipeline
- Aucun secret, mot de passe, ou clé committé dans ce repository

---

## Limites connues et améliorations possibles

- Traitement actuellement full-reload en silver/gold (pas d'incrémental) — à optimiser si le volume grandit
- Pas de planification automatique (déclenchement manuel du pipeline ADF)
- Pas d'alerting en cas d'échec d'une tâche
- Modèle en étoile limité à un seul domaine métier (`ventes`) — architecture prête à accueillir d'autres domaines via de nouveaux schémas

---

## Données

Les fichiers sources (`ventes.csv`, `retours.csv`) sont **entièrement synthétiques**, générés pour ce projet avec des défauts de qualité injectés volontairement (valeurs manquantes, incohérences de casse, doublons, références orphelines) afin de démontrer les mécanismes de contrôle qualité.

---

## Auteur

Joseph Patrick MBARGA MESSI (Jopa) — Data Engineer
