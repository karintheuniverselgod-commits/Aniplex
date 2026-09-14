FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    aria2 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /app/logs /app/anime_downloads /app/thumbnails && \
    chown -R appuser:appuser /app/logs /app/anime_downloads /app/thumbnails

USER appuser

# HF Spaces requires port 7860
ENV PORT=7860
EXPOSE 7860

# Health check for HF Spaces
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:7860/health || exit 1

# Run the bot
CMD ["python", "bot.py"]
