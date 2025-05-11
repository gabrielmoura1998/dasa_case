FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestao.py .
COPY tratamento.py .
ENV PYTHONUNBUFFERED=1
CMD ["sh", "-c", "python ingestao.py && python tratamento.py"]