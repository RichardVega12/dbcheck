FROM python:3.11-slim

# Instala dependencias del sistema (incluyendo las necesarias para PyArrow)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY . /app
WORKDIR /app

# Variables de entorno para desarrollo
ENV FLASK_DEBUG=true
ENV FLASK_ENV=development

# Comando condicional: Desarrollo con auto-reload vs Producción con Gunicorn
CMD ["sh", "-c", "if [ \"$FLASK_DEBUG\" = \"true\" ]; then python -m flask run --host=0.0.0.0 --port=5000 --reload; else gunicorn app_unificado:app --bind 0.0.0.0:5000 --workers 2 --threads 4; fi"]