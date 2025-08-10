FROM astrocrpublic.azurecr.io/runtime:3.0-5

RUN pip install uv

COPY pyproject.toml uv.lock ./

RUN uv export --no-dev --all-extras --no-hashes > requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY .env ./
COPY dags/ ./dags/
COPY plugins/ ./plugins/
COPY include/ ./include/
COPY airflow_settings.yaml ./

RUN pip install -e .
