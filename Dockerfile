FROM python:3.11-slim

WORKDIR /app

# MODIFICADO-Instalar dependencias y Chromium
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libnss3 \
    libxss1 \
    libatk1.0-0 \
    libgtk-3-0 \
    fonts-liberation \
    libasound2 \
    xdg-utils \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Zona horaria del contenedor (TZ) y del scheduler (TIMEZONE)
ENV TZ=America/Lima
ENV TIMEZONE=America/Lima

CMD ["python", "src/main.py"]
