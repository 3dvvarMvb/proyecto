# Usar imagen con compiladores incluidos
FROM python:3.11

WORKDIR /app

# Actualizar pip
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "client.py"]
