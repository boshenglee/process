import sys

import config 
import manual as manual
import basic as auto

if __name__ == "__main__":

    if config.INSTRUMENTATION:
        app = manual.create_app()
    else:
        app = auto.create_app()
        
    app.run(host=config.FLASK_HOST, port=config.FLASK_PORT)