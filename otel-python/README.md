A repository to instrument python app with OpenTelemtry

basic.py 
    To test auto-instrumentation without any code change

manual.py 
    Code change to implement manual-instrunetation


To Run:
    python wsgi.py

TO Run With Auto Instrumentation(Without Collector):
    export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
    opentelemetry-instrument \
        --traces_exporter console \
        --metrics_exporter console \
        --logs_exporter console \
        --service_name dice-server \
        python wsgi.py

TO Run With Auto Instrumentation(With Collector):
    export OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
    opentelemetry-instrument \
        --traces_exporter otlp,console \
        --metrics_exporter otlp,console \
        --logs_exporter otlp,console \
        --service_name dice-server \
        python wsgi.py

To Build Docker Image:
    Normal Image(Without Instrumentation):
        $ docker build -f Dockerfile.latest -t otel-python:latest .
    Instrumented Images
        $ docker build -f Dockerfile.auto -t otel-python:auto .
