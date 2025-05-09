FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestao.py .
ENV PYTHONUNBUFFERED=1
CMD \["python", "ingestao.py"]