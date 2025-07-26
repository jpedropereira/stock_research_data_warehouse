FROM astrocrpublic.azurecr.io/runtime:3.0-5

RUN pip install uv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY . .