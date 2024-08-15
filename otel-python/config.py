import os
from dotenv import load_dotenv

load_dotenv()

FLASK_HOST = os.getenv("FLASK_HOST")
FLASK_PORT = int(os.getenv("FLASK_PORT"))

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME")

INSTRUMENTATION = int(os.getenv("INSTRUMENTATION"))

OTLP_EXPORT_IP = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

