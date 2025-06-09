FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libsasl2-2 \
    libssl-dev \
    libffi-dev \
    librdkafka-dev \
    python3-dev \
    openssl \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV for modern Python package management
RUN pip install --no-cache-dir uv

# Set working directory to match k8s PYTHONPATH expectations
WORKDIR /opt/testdatapy

# Copy all project files for complete package installation
COPY . .

# Install dependencies and package using UV
RUN uv pip install --system -e .

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /opt/testdatapy
USER appuser

# Default command
CMD ["testdatapy", "--help"]
