FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy migration module (shared with Lambda handlers)
COPY ../migration ./migration/

# Copy worker code
COPY worker.py .

# Non-root user for security
RUN useradd -r -s /bin/false worker
USER worker

ENTRYPOINT ["python", "-u", "worker.py"]
