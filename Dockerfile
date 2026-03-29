FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download FinBERT weights at build time so they're baked into the image.
# This avoids HuggingFace rate-limiting Cloud Run's shared IP at runtime.
RUN python -c "from transformers import pipeline; pipeline('text-classification', model='ProsusAI/finbert', tokenizer='ProsusAI/finbert')"

COPY . .

ENV PORT=8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 300 main:pipeline_entry
