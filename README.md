# Big Data CHU

Ce projet propose une infrastructure moderne d'ingestion, de nettoyage, d'analyse et de visualisation de données médicales (patients, séjours, diagnostics et constantes de monitoring) pour un Centre Hospitalier Universitaire (CHU).

Il met en œuvre une **architecture Médaillon** avec une stack combinant **Python (géré par uv)**, **ClickHouse** et **Metabase** (adossé à **PostgreSQL**).

---

## Architecture des Données (Pipeline Médaillon)

Le traitement des données suit un flux en plusieurs étapes :

```text
file_storage/ (Données brutes)
       │
       ▼ (Génération & Filtrage)
   lake/ (Data Lake Local)
       │
       ▼ (Ingestion brute)
  chu.xxx_bronze (ClickHouse)
       │
       ▼ (Nettoyage, Déduplication, Anonymisation RGPD)
  chu.xxx_silver (ClickHouse)
       │
       ▼ (Métriques & Agrégations Métiers)
  chu.xxx_gold (ClickHouse)
```

### 1. Ingestion Locale & Filtrage (`generating-lake`)
- **Source** : `file_storage/`
- **Cible** : `lake/`
- **Rôle** : Simule l'ingestion quotidienne incrémentale dans le Data Lake local. Seuls les dossiers plus récents que la date enregistrée dans `config/last_lake_generation.csv` sont traités.
- **Sécurité & Nettoyage initial** : Pour les patients, seules les colonnes d'intérêt (`patient_id`, `birth_date`, `sex`) sont conservées pour limiter l'exposition de données sensibles (RGPD). Les autres types de fichiers (séjours, diagnostics, constantes, référentiels) sont copiés à l'identique.

### 2. Étape Bronze (`bronze`)
- **Source** : `lake/`
- **Cible** : Tables ClickHouse `*_bronze` (ex: `patients_bronze`, `sejours_bronze`, etc.)
- **Rôle** : Chargement direct et structuré des fichiers CSV, JSON (diagnostics) et Parquet (monitoring) dans ClickHouse. Des colonnes de traçabilité (`inserted_at` et `data_path`) sont ajoutées à chaque ligne.

### 3. Étape Silver (`silver`)
- **Source** : Tables `*_bronze`
- **Cible** : Tables ClickHouse `*_silver`
- **Rôle** : Nettoyage et fiabilisation des données :
  - **Filtres de cohérence** : Suppression des patients avec des genres invalides (différents de `'F'` ou `'M'`) ou des dates de naissance futures, suppression des séjours où la date de sortie précède l'admission.
  - **Correction des capteurs** : Mise à `NULL` des constantes de monitoring hors limites (ex: fréquence cardiaque < 20 ou > 250 bpm, SpO2 < 50%, température < 30°C ou > 45°C).
  - **Déduplication** : Application de la clause `DEDUPLICATE` de ClickHouse sur les clés primaires métiers.
  - **Conformité RGPD / Anonymisation** : Suppression automatique des petites cohortes (effectifs de patients < 5) sur les diagnostics et séjours pour empêcher la ré-identification de patients.

### 4. Étape Gold (`gold`)
- **Source** : Tables `*_silver`
- **Cible** : Tables ClickHouse `*_gold`
- **Rôle** : Modélisation analytique prête pour le reporting et la BI :
  - **DMS (Durée Moyenne de Séjour)** par service.
  - **Flux journaliers** : Nombre de patients admis par jour et par service.
  - **Taux de réadmission à 30 jours** : Calcul global et par service.
  - **Analyse démographique** : Répartition par groupe d'âge et sexe (avec masquage des cohortes de taille < 5).
  - **Alertes de monitoring** : Synthèse quotidienne des alertes physiologiques (bradycardie, tachycardie, désaturation en O2, fièvre).
  - **Recherche clinique** : Regroupement des cohortes par diagnostic CIM10.

---

## 🛠️ Stack Technique

- **Langage & Environnement** : Python >= 3.13, orchestré via [uv](https://github.com/astral-sh/uv).
- **Base de Données Analytique** : [ClickHouse](https://clickhouse.com/) (port de connexion : `18123` pour HTTP).
- **Visualisation / BI** : [Metabase](https://www.metabase.com/) (port : `3000`).
- **Base Métadonnées Metabase** : [PostgreSQL 16](https://www.postgresql.org/).
- **Conteneurisation** : Docker & Docker Compose.

---

## Structure du Projet

```text
big_data/
├── config/
│   └── last_lake_generation.csv  # Horodatage de la dernière ingestion incrémentale
├── file_storage/                 # Dossier source contenant les données brutes (patients, séjours, etc.)
├── lake/                         # Data Lake intermédiaire filtré et structuré
├── pg_data/                      # Stockage persistant PostgreSQL pour Metabase
├── src/
│   ├── big_data/                 # Configuration et connexion ClickHouse
│   │   └── clickhouse_config/
│   │       └── clickhouse.py
│   └── scripts/                  # Code source des scripts de la pipeline
│       ├── generating_lake.py    # Ingestion incrémentale file_storage -> lake
|       ├── cron.py               # Script contenant le job lancé par le scheduler
|       ├── scheduler.py          # Script qui lance le scheduler de cronjob
│       ├── bronze.py             # Ingestion lake -> ClickHouse Bronze
│       ├── silver.py             # Nettoyage et mise en conformité Silver
│       └── gold.py               # Modélisation et KPI analytiques Gold
├── docker-compose.yml            # Services Docker (ClickHouse, PostgreSQL, Metabase)
├── pyproject.toml                # Configuration du projet, dépendances et raccourcis de scripts uv
├── uv.lock                       # Lockfile des dépendances Python (uv)
└── README.md                     # Documentation générale du projet
```

---

## Démarrage & Utilisation

### Prérequis
Assurez-vous d'avoir installé sur votre machine :
- **Docker** & **Docker Compose**
- **Python 3.13+** (ou simplement [uv](https://github.com/astral-sh/uv))

Assurez vous également d'avoir ajouté le dossier `file_storage` contenant les données brutes à la racine de ce repo.

---

### 1. Configuration des variables d'environnement

Copiez le fichier exemple `.env.example` vers un nouveau fichier `.env` :

```bash
# Sur Linux / macOS / Git Bash
cp .env.example .env

# Sur Windows (PowerShell)
copy .env.example .env
```

Puis, éditez `.env` pour y renseigner vos variables locales de connexion (par exemple pour l'accès local à ClickHouse en mode de développement) :
```env
CLICKHOUSE_USERNAME="root"
CLICKHOUSE_PASSWORD="root"
CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="18123"
CLICKHOUSE_DB="chu"
```

---

### 2. Lancement des Services

Lancez la stack de bases de données et d'outils BI en arrière-plan :

```bash
docker compose up -d
```

Pour vérifier que les conteneurs fonctionnent correctement :
```bash
docker compose ps
```

Le cronjob va lancer le pipeline médaillon de la génération du lake jusqu'au gold. Le job se lance tous les jours à 1h00 du matin (on part du principe que l'export des données se fait à minuit). Lancer le cronjob :
```bash
uv run scheduler
```

---

### 3. Installation des Dépendances Python

Nous utilisons `uv` pour synchroniser l'environnement virtuel de manière optimale. Si vous n'avez pas installé `uv`, installez-le avec `pip install uv`.

```bash
# Synchronise et installe toutes les dépendances dans un environnement virtuel propre (.venv)
uv sync
```

---

### 4. Exécution de la Pipeline de Données

Exécutez séquentiellement les étapes de traitement de données grâce aux raccourcis définis dans le `pyproject.toml` :

```bash
# Étape 0 : Ingestion brute filtrée depuis file_storage vers le lake
uv run generating-lake

# Étape 1 : Chargement des données du lake dans la couche Bronze ClickHouse
uv run bronze

# Étape 2 : Nettoyage, déduplication et application des contraintes RGPD dans la couche Silver
uv run silver

# Étape 3 : Calcul des agrégats et indicateurs dans la couche Gold
uv run gold
```

### 5. Utiliser Metabase

A la fin du formulaire d'inscription de Metabase, on vous demandera de choisir votre source de données.

- Choisir Clickhouse
- Host: clickhouse
- Port : 8123
- les credentials sont ceux utilisés pour clickhouse
