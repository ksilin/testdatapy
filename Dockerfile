FROM python:3.11-slim

# Install build dependencies
# Install native dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libsasl2-2 \
    libssl-dev \
    libffi-dev \
    librdkafka-dev \
    python3-dev \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Install confluent-kafka[avro]
RUN pip install --no-cache-dir "confluent-kafka[avro]>=2.3.0"

# Install remaining dependencies (excluding confluent-kafka from requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Install the package
RUN pip install --no-cache-dir -e .

# Create a non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command
CMD ["testdatapy", "--help"]
