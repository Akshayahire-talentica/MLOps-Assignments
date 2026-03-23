# MLOps POC - Recommendation Model Service
# Production-grade Dockerfile with multi-stage build

# ============================================================
# Stage 1: Builder
# ============================================================
FROM python:3.9-slim AS builder

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies for building
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# ============================================================
# Stage 2: Runtime
# ============================================================
FROM python:3.9-slim

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    MLFLOW_HOME=/app/.mlflow \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64 \
    LOG_LEVEL=INFO

# Create non-root user for security
RUN useradd -m -u 1000 mlops && \
    mkdir -p /app/.mlflow && \
    chown -R mlops:mlops /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy application code
COPY --chown=mlops:mlops src/ /app/src/
COPY --chown=mlops:mlops config/ /app/config/
COPY --chown=mlops:mlops data/ /app/data/

# Switch to non-root user
USER mlops

# Expose ports
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Labels for metadata
LABEL org.opencontainers.image.title="MLOps POC API Service" \
    org.opencontainers.image.description="FastAPI service for recommendation model serving" \
    org.opencontainers.image.version="1.0.0" \
    org.opencontainers.image.source="https://github.com/Akshayahire-talentica/MLOps-POC-1"

# Run FastAPI service
CMD ["python", "-m", "uvicorn", "src.serving.api_service:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
