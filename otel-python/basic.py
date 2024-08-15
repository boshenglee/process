import time
from random import randint
from flask import Flask, request
import logging


dice = 6

def create_app():

    app = Flask(__name__)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    @app.route("/rolldice")
    def roll_dice():
        player = request.args.get('player', default=None, type=str)
        result = str(roll())
        if player:
            logger.warning("%s is rolling the dice: %s", player, result)
        else:
            logger.warning("Anonymous player is rolling the dice: %s", result)
        return result
    
    @app.route("/changedice",methods=["POST"])
    def new_dice():
        global dice 

        number = request.args.get('number', type=int)
        dice = number
        logger.info(f"Change a new dice with {number} number")
        return str(dice)


    def roll():
        return randint(1, dice)
    
    return app