FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY check_names.py web_server.py ./
COPY sources/ ./sources/
COPY web/ ./web/
COPY companies_example.csv ./

EXPOSE 8080
CMD ["python", "web_server.py", "--host", "0.0.0.0"]
