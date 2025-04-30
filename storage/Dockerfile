FROM python:3.11-slim
WORKDIR /app
# instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# copiar código
COPY . .
EXPOSE 5000
CMD ["python", "client.py"]