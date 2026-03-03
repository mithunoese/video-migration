FROM python:3.12-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for bcrypt
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dashboard/ dashboard/
COPY migration/ migration/
COPY public/ public/
COPY api/ api/
COPY run.py run_dashboard.py ./

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser && \
    mkdir -p /app/data && chown -R appuser:appuser /app

# State directory inside the container (persisted via volume)
ENV STATE_DIR=/app/data

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/status')" || exit 1

CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
