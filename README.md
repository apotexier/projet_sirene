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


## 🏗️ Architecture des Données
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