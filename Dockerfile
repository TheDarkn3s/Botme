FROM python:3.10-slim

# 1) Instala dependencias de sistema para compilar (opcional)
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) Copia e instala tus dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3) Copia el resto de tu código
COPY . .

# 4) Arranca tu bot
CMD ["python", "main.py"]
