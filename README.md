# 🏭 SIRENE Data Pipeline

## 🎯 Objectif & Description
Ce projet python implémente un pipeline ETL (Extract, Transform, Load) conçu pour traiter les données massives du répertoire SIRENE (Insee).

Le pipeline convertit des millions de lignes brutes en une Table enrichie et en KPIs, en appliquant des filtres métiers (ex: focus Île-de-France) et une validation rigoureuse de la qualité des données.

## ⚙️ Installation
Ce projet utilise uv pour une gestion extrêmement rapide de l'environnement et des dépendances.

1. Cloner le projet :
    ```
   git clone https://github.com/apotexier/projet_sirene.git
   cd projet_sirene
    ```

2. Synchroniser l'environnement :
   ```
   uv sync
   ```

## 📂 Architecture du Projet

Le projet est structuré de manière modulaire pour séparer la logique de traitement, la configuration et les données. Voici le détail de l'arborescence :

| Dossier / Fichier | Description |
| :--- | :--- |
| **`.venv/`** | Environnement virtuel isolé contenant les 111 packages gérés par **uv**. |
| **`data/`** | Stockage local des données structuré selon l'architecture Medallion (**bronze**, **silver**, **gold**). |
| **`docs/`** | Contient la documentation technique et le support de présentation (Pipeline SIRENE.pptx). |
| **`notebooks/`** | Travaux d'exploration des données et prototypage des calculs SQL DuckDB. |
| **`scripts/`** | Utilitaires de maintenance : `check_quality.py` (Linting/Typage) et `check_gold.py` (Validation des KPIs). |
| **`src/`** | Cœur du pipeline : contient les définitions des jobs pour chaque couche et les services métier. |
| **`tests/`** | Suite de tests unitaires et d'intégration validant l'idempotence et la logique des KPIs via **Pytest**. |
| **`.env`** | Fichier de variables d'environnement (ex: `ENV_FOR_DYNACONF`) pour basculer entre Prod et Dev. |
| **`pyproject.toml`** | Configuration centrale du projet (dépendances, outils Ruff, Mypy et Pytest). |
| **`uv.lock`** | Empreinte exacte des dépendances pour garantir la reproductibilité sur n'importe quelle machine. |
| **`README.md`** | Guide d'installation et documentation générale du projet. |
Le projet suit l'architecture Medallion, garantissant une traçabilité totale :

🥉 Bronze (Raw) : Ingestion incrémentale des fichiers Parquet originaux.

🥈 Silver (Cleaned) : Nettoyage, typage strict, enrichissement (calcul d'âge, secteurs) et validation de schémas via Pandera.

🥇 Gold (Analytics) : Jointures et agrégations SQL avec DuckDB pour générer les KPIs.

#### 🛠️ Stack Technique

* Moteur de calcul : DuckDB 
  
* Cleaning & Validation : Pandera & Pandas

* Configuration : Dynaconf (Gestion multi-environnements)

* environnement de projet : uv (Ultra-fast Python bundler)

* Logs : Loguru

* Test : pytest

* Stockage : parquet

#### 🏞️ Environnements
Le comportement du pipeline se configure via le fichier .env à la racine :

* Production **(ENV_FOR_DYNACONF=production)** : Traitement complet des données.

* Développement **(ENV_FOR_DYNACONF=development)** : Utilise la sample_limit définie dans *config/settings.toml* pour des tests rapides.

### 🧱 Lancer le pipeline complet

Exécutez l'ensemble du cycle de données de bout en bout :
```bash
uv run python -m sirene_pipeline.main
```

### 👌 Qualité du code et Tests
Le projet impose des standards de qualité automatisés :
* Linting & Formatage : lancer le fichier check_quality.py

* Test : lancer pytest dans le dossier *tests*


### 💾 Auteur:
*atexier - Février 2026*