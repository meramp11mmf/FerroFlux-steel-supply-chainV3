
FROM python:3.11-slim

WORKDIR /app

# install deps first (uses PyPI, not apt)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy application code (build context = ./portal)
COPY app /app/app

EXPOSE 8000

# healthcheck via python (no apt/curl needed — works on networks that block apt)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
