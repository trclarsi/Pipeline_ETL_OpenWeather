# On part d'une image Python simple et propre
FROM python:3.9-slim

#USER airflow
# On définit un répertoire de travail
WORKDIR /app

# On copie le fichier des dépendances
COPY requirements.txt .

# On installe les dépendances
# Note: On n'installe plus pyspark ici, car il sera dans sa propre image
RUN pip install --no-cache-dir -r requirements.txt

# On copie notre code source
COPY src/ .

# Cette image ne sera pas lancée directement, elle sera utilisée par Airflow.