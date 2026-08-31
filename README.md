# 🏥 Big Data CHU (Centre Hospitalier Universitaire)

Ce projet propose une infrastructure moderne d'ingestion, de nettoyage, d'analyse et de visualisation de données médicales (patients et séjours) pour un Centre Hospitalier Universitaire (CHU).

Il s'appuie sur une stack technique performante combinant le traitement de données en **Python**, une base de données analytique orientée colonne (**ClickHouse**), et une plateforme de Business Intelligence (**Metabase** adossée à **PostgreSQL**).

---

## 🛠️ Stack Technique

- **Langage & Traitement** : Python >= 3.13 (géré efficacement par [uv](https://github.com/astral-sh/uv))
- **Base de Données Analytique (OLAP)** : [ClickHouse](https://clickhouse.com/) (pour des requêtes analytiques ultra-rapides sur de grands volumes)
- **Visualisation & Business Intelligence** : [Metabase](https://www.metabase.com/)
- **Base Métadonnées Metabase** : [PostgreSQL 16](https://www.postgresql.org/)
- **Conteneurisation** : Docker & Docker Compose

---

## 📂 Structure du Projet

```text
big_data/
├── file_storage/           # Stockage local des données brutes (patients, séjours)
├── pg_data/                # Volume persistant pour la base de données PostgreSQL (Metabase)
├── scripts/                # Scripts utilitaires Python de nettoyage et d'analyse
│   ├── delete_people.py    # Nettoyage et filtrage des fichiers patients
│   └── detect_weirdo.py    # Détection d'anomalies/données manquantes dans les séjours
├── src/
│   └── big_data/           # Package Python principal
│       └── __init__.py     # Point d'entrée de l'application
├── docker-compose.yml      # Configuration des services Docker (ClickHouse, PostgreSQL, Metabase)
├── pyproject.toml          # Configuration du projet Python (dépendances et scripts)
├── uv.lock                 # Fichier de verrouillage des dépendances Python (uv)
└── README.md               # Documentation du projet
```

---

## 🚀 Démarrage Rapide

### Prérequis
Assurez-vous d'avoir installé sur votre machine :
- **Docker** & **Docker Compose**
- **Python 3.13+** (l'outil de gestion de packages et d'environnements virtuel [uv](https://github.com/astral-sh/uv) est fortement recommandé)

---

### 1. Lancement de l'Infrastructure Docker

Les bases de données et l'outil de BI sont orchestrés via Docker Compose. Pour démarrer les services en arrière-plan :

```bash
docker compose up -d
```

#### Services déployés :
* **ClickHouse Server** :
  * **Port HTTP** : `18123` (mappé depuis le port interne `8123`)
  * **Port TCP (client natif)** : `19000` (mappé depuis le port interne `9000`)
  * **Utilisateur** : `root`
  * **Mot de passe** : `root`
  * **Base de données par défaut** : `chu`
* **Metabase** :
  * Accessible à l'adresse : [http://localhost:3000](http://localhost:3000)
* **PostgreSQL 16** (Base interne de Metabase) :
  * **Utilisateur** : `metabase`
  * **Mot de passe** : `mysecretpassword`
  * **Base de données** : `metabaseappdb`

Pour vérifier que tous les services tournent correctement :
```bash
docker compose ps
```

---

### 2. Configuration de l'Environnement Python

Nous recommandons l'utilisation de `uv` pour une installation rapide des dépendances et de l'environnement virtuel.

#### Avec `uv` :
```bash
# Installer les dépendances et synchroniser l'environnement virtuel
uv sync

# Lancer le script principal du package
uv run big-data
```

#### Avec pip classique (standard) :
```bash
# Créer l'environnement virtuel
python -m venv .venv

# Activer l'environnement virtuel
# Sur Windows (PowerShell) :
.venv\Scripts\Activate.ps1
# Sur macOS/Linux :
source .venv/bin/activate

# Installer le package local
pip install -e .

# Lancer l'application principale
python -m src.big_data
```

---

## 📊 Scripts Utilitaires (`scripts/`)

Le dossier `scripts` contient des outils dédiés à la manipulation et au nettoyage des fichiers bruts situés dans le dossier `file_storage`.

### 1. Nettoyage des patients (`scripts/delete_people.py`)
Ce script parcourt récursivement les dossiers de patients sous `file_storage/patients/`, extrait uniquement les colonnes d'intérêt (ID, Nom, Prénom, Sexe - indices `0`, `4`, `5`, `6`), puis génère un fichier CSV de sortie nettoyé nommé `patients_clean.csv` au même emplacement.

Pour l'exécuter :
```bash
uv run python scripts/delete_people.py
# ou
python scripts/delete_people.py
```

### 2. Détection d'anomalies de séjours (`scripts/detect_weirdo.py`)
Ce script analyse le fichier de séjours d'une date spécifique (`file_storage/sejours/2026-08-26/sejours.csv`) pour identifier les lignes dites "anormales" (dont le dernier champ est manquant/vide, signalant potentiellement un séjour non clôturé ou corrompu).

Pour l'exécuter :
```bash
uv run python scripts/detect_weirdo.py
# ou
python scripts/detect_weirdo.py
```

---

## 📈 Exploration et Visualisation (Metabase)

1. Ouvrez votre navigateur sur [http://localhost:3000](http://localhost:3000).
2. Configurez votre compte administrateur initial.
3. Connectez Metabase à ClickHouse (ou PostgreSQL) pour commencer à construire des tableaux de bord interactifs (admissions, démographie des patients, durée des séjours, anomalies, etc.).

