import time,config, threading
from datetime import datetime
from queue import Queue

# These are the necessary import declarations
from opentelemetry import trace
from opentelemetry import metrics


from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
# from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter
# from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter


from random import randint
from flask import Flask, request
import logging

message_queue = Queue()

def create_app():

    resource = Resource(attributes={
        SERVICE_NAME: config.SERVICE_NAME
    })

    # logs
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    exporter = OTLPLogExporter(insecure=True)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    handler = LoggingHandler(level=logging.DEBUG, logger_provider=logger_provider)
    logging.getLogger().addHandler(handler)
    roll_log = logging.getLogger("rolls_logs")
    roll_log.setLevel(logging.DEBUG)
    dice_log = logging.getLogger("dice_logs")
    dice_log.setLevel(logging.DEBUG)
    player_log = logging.getLogger("player_logs")
    player_log.setLevel(logging.DEBUG)


    # tracer
    provider = TracerProvider(resource=resource)
    # processor = BatchSpanProcessor(ConsoleSpanExporter())
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{config.OTLP_EXPORT_IP}"))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)

    # metrix
    # reader = PeriodicExportingMetricReader(
    #     ConsoleMetricExporter(),
    #     export_interval_millis=10000)
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{config.OTLP_EXPORT_IP}"),
        export_interval_millis=10000)
        
    meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meterProvider)
    meter = metrics.get_meter("diceroller.meter")



    # Now create a counter instrument to make measurements with
    roll_counter = meter.create_counter(
        "dice.rolls",
        description="The number of rolls by roll value",
    )

    big_value_counter = meter.create_counter(
        "dice.big",
        description="The nuber of rolls value bigger than 3",
    )
    
    small_value_counter = meter.create_counter(
        "dice.small",
        description="The nuber of rolls value smaller or equal than 3",
    )

    # Define a histogram to measure request durations
    request_duration_histogram = meter.create_histogram(
        name="request_duration",
        unit="ms",
        description="The duration of the requests in milliseconds",
    )

    app = Flask(__name__)

    # create a new thread to handle message 

    

    @app.before_request
    def before_request():
        request.start_time = datetime.now()

    @app.after_request
    def after_request(response):
        end_time = datetime.now()
        duration_ms = (end_time - request.start_time).total_seconds() * 1000  # Convert to milliseconds
        request_duration_histogram.record(duration_ms,{'request.path':request.path})
        print(f"Request handled in {duration_ms} ms")
        return response

    @app.route("/rolldice")
    def roll_dice():
        # This creates a new span that's the child of the current one
        with tracer.start_as_current_span("roll") as roll_span:
            player = request.args.get('player', default = None, type = str)
            result = str(roll())
            roll_span.set_attribute("roll.value", result)
            # This adds 1 to the counter for the given roll value
            roll_counter.add(1, {"roll.value": result})
            carrier = {}
            # Write the current context into the carrier.
            TraceContextTextMapPropagator().inject(carrier)

            if player:
                player_register(player)
                roll_log.info(f"{player} is rolling the dice: {result}")
                carrier["message"] = f"{player} is rolling the dice: {result}"
                message_queue.put(carrier)
                return result
            else:
                roll_log.info("Anonymous player is rolling the dice: %s", result)
                carrier["message"] = f"Anonymous player is rolling the dice: {result}",
                message_queue.put(carrier)
                return result

    @app.route("/changedice",methods=["POST"])
    def new_dice():
        with tracer.start_as_current_span("change_dice") as change_span:
            global dice 

            number = request.args.get('number', type=int)
            dice = number
            dice_log.info(f"Change a new dice with {number} number")
            return str(dice)
    
    def player_register(player):
        with tracer.start_as_current_span("roll_player") as player_span:
            player_span.set_attribute("roll.player",player)
            player_log.info(player)

        
    def roll():
        rdm = randint(1,6)
        if rdm >3:
            big_value_counter.add(1)
        else:
            small_value_counter.add(1)
        return rdm
    

    def run():

        global message_queue

        while True:
            if(len(message_queue.queue) != 0):
                carrier = message_queue.get()
                ctx = TraceContextTextMapPropagator().extract(carrier=carrier)

                with tracer.start_as_current_span('message_queue', context=ctx) as span:
                    span.set_attribute('Send Message Time', int(time.time() * 1000))
                    print(f"Send Message: {carrier['message']}")   
    
    t = threading.Thread(target=run)
    t.start()

    return app