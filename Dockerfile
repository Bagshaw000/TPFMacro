# --- Builder phase set up python environment

FROM python:3.13-slim AS builder

WORKDIR /macro

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install --prefix=/install -r requirements.txt


# --- Runtime
FROM python:3.13-slim

WORKDIR /macro

# Install Doppler CLI + runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    ca-certificates \
    gnupg \  
    && rm -rf /var/lib/apt/lists/*

# Install Doppler CLI using their official install script (always up to date)
RUN curl -Ls --tlsv1.2 --proto "=https" \
    https://cli.doppler.com/install.sh | sh
# Add Doppler to PATH (it installs to /root/.doppler/bin/doppler by default)
ENV PATH="/root/.doppler/bin:${PATH}"

COPY --from=builder /install /usr/local
COPY . .

RUN touch __init__.py \
         /src/__init__.py \ 
         || true

RUN useradd -m -u 1001 appuser && chown -R appuser:appuser /macro
USER appuser

EXPOSE 8000

# Default: FastAPI. Override `command` in docker-compose per service.
CMD ["doppler", "run","--","uvicorn", "main:macro", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]