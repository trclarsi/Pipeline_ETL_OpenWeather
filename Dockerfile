# Dockerfile.airflow
FROM apache/airflow:2.8.0-python3.9

# Basculer vers l'utilisateur root pour installer des packages système
USER root

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Basculer vers l'utilisateur airflow
USER airflow

# Copier et installer les requirements Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Créer les répertoires nécessaires
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copier le code source
COPY --chown=airflow:airflow src/ /opt/airflow/

# Définir le répertoire de travail
WORKDIR /opt/airflow