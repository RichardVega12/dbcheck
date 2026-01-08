FROM python:3.11-slim

# Instala dependencias del sistema (incluyendo las necesarias para PyArrow y build)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements y instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código fuente
COPY . /app

# Cambia el directorio de trabajo (WORKDIR) a /app
WORKDIR /app

# Configuración para producción en Render (no necesitas FLASK_DEBUG ni FLASK_ENV aquí)
# Render siempre define PORT, así que usamos directamente $PORT

# Comando de inicio: SIEMPRE producción con Gunicorn (mejor para Render)
# -w 2 → 2 workers (suficiente para plan free, ajusta si usas plan pagado)
# --threads 4 → threads por worker (opcional, pero ayuda con I/O)
# --bind 0.0.0.0:$PORT → OBLIGATORIO para Render
CMD exec gunicorn --bind=0.0.0.0:$PORT --workers=2 --threads=4 app_unificado:app
