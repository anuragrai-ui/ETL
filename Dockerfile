FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirement.txt .
RUN pip install -r requirement.txt

COPY . .

# Serves jira_data_loader from main.py on $PORT (Cloud Run sets PORT=8080)
CMD exec functions-framework --target=jira_data_loader --source=main.py --port=${PORT:-8080}
