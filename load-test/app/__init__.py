import binascii
import time
import uuid
import random 
import threading
import logging 
import datetime

from flask import Flask, request
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement


sc_logs = [
    "[954821740] PCT2075: TEMPERATURE : MB[42.37 C] PMS[44.25 C]",
    "[954821740] MSEQ_V3: Motor speeds non-zero on arrival: A=11 B=6 C=14 D=11 : Initiating force stop of all motors.",
    "[954821740] MSEQ_V3: | SCurve:16 SA:11 SB:6 SC:14 SD:11 | CA:-332 CB:-109 CC:289 CD:-238 | EA:0 EB:0 DA:0.00 DB:0.00 | EST_D:1590.51",
    "[954821740] MSEQ_V3: Motor speeds non-zero on arrival: A=6 B=2 C=4 D=7 : Initiating force stop of all motors.",
    "[954821740] MSEQ_V3: ARRIVAL CONFIRMED - Cube: 2, Axis: MCUBE_Y, Dir: MCUBE_FORWARD",
    "[954821740] MSEQ_V3: Estimated Payload: Total-[PAYLOAD_BIN_10KG] A-[7][PAYLOAD_BIN_10KG][CURR_A: 19.5100A] B-[11][PAYLOAD_NONE][CURR_B: 15.1700A]",
    "[954821740] MSEQ_V3: MOTOR A: ANGLE:1396 | DISTANCE: 2010.10 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-416.82 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
    "[954821740] MSEQ_V3: MOTOR B: ANGLE:1388 | DISTANCE: 1998.58 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-405.30 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
    "[954821740] MSEQ_V3: MOTOR C: ANGLE:1384 | DISTANCE: 1992.82 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-399.54 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
    "[954821740] MSEQ_V3: MOTOR D: ANGLE:1382 | DISTANCE: 1989.94 mm | ESTIMATE D:1593.27 mm | TARGET D:2020.00 mm | ERROR D:-396.66 mm | ENCA:0.00 mm ENCB:0.00 mm | CUBE:2",
    "[954821740] MCUBE_SEQ: [CNT:4] MCUBE ID PAIR Y [8][3][7][4]",
    "[954821740] MCUBE_SEQ: MOTOR STATUS CHECK [(0)ID:8,498V,32C,0] [(0)ID:3,498V,31C,0] [(0)ID:7,504V,31C,0] [(0)ID:4,497V,31C,0]",
    "[954821740] WEX_SEQ: MOTOR STATUS CHECK [(0)ID:11,502V,31C,0] [(0)ID:12,502V,29C,0]",
    "[954821740] WEX_SEQ: WEX_CMD_STATUS2: [A:11][SPEED:0][TEMP:31][IQ:-31][ENC:65473]",
    "[954821740] WEX_SEQ: WEX_CMD_STATUS2: [B:12][SPEED:0][TEMP:29][IQ:-36][ENC:65473]",
    "[954821740] MASTER_SEQ: RIGHT : 1, LEFT : 1, FRONT : 1, BACK : 1",
    "[954821740] MASTER_SEQ: CURRENTLY ALIGN AT XY",
    "[954821740] WEX_SEQ_V2: WEX CALIBRATION MODE POS",
    "[954821740] WEX_SEQ_V2: WEX A [x:0][y:1][xy:0][xyc:0]",
    "[954821740] WEX_SEQ_V2: WEX B [x:0][y:1][xy:0][xyc:0]",
    "[954821740] WEX_SEQ_V2: WEX CURRENTLY AT Y",
    "[954821740] WEX_SEQ_V2: WEX [xy] CURRENTLY AT Y, GOING TO XY [ANGLE:-10]",
    "[954821740] WEX_SEQ_V2: WEX [xy] ENCODER START POSITION [ENCODER A:6399] [ENCODER B:6399]"
]

DB = [
    'skycar_logs_with_index',
    'skycar_logs_without_index',
    'skycar_logs_with_partition'
]


lines = 10
indexing = False

db_name = DB[2]

# Create a custom logger
formatter = logging.Formatter("%(asctime)s.%(msecs)03d - %(name)s - [%(levelname)s] - %(message)s -  %(funcName)s", datefmt='%Y-%m-%d %H:%M:%S')

stats_log = logging.getLogger('statistics')
stats_log.setLevel(logging.DEBUG)
# Create a file handler
stats_file_handler = logging.FileHandler('statistics.log')
stats_file_handler.setLevel(logging.DEBUG)
# Create a formatter and add it to the handler
stats_file_handler.setFormatter(formatter)
# Add the handler to the logger
stats_log.addHandler(stats_file_handler)


def create_app():

    app = Flask(__name__)

    # cassandra
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster  = Cluster(['127.0.0.1'],port=9042,auth_provider=auth_provider)
    session = cluster.connect('skycar_logs')

    print(f'Lines : {lines}')
    print(f'DB : {db_name}')

    @app.route("/change-lines",methods=["POST"])
    def change_lines():
        global lines
        lines = request.args.get('lines', type=int)
        return {"lines":lines}
    
    @app.route("/change-db",methods=["POST"])
    def change_db():
        global db_name

        db_index = request.args.get('db', type=int)

        db_name = DB[db_index]

        return db_name
    
    @app.route("/start")
    def start():
        t = threading.Thread(target=run, args=(lines,))
        t.start()

        return "200"
    
    @app.route("/skycar-logs")
    def get_skycar_logs():
        paging_state = cassandra_read_from_db()

        return {"paging_state":binascii.hexlify(paging_state).decode()}
    
    @app.route("/skycar-logs-with-filter")
    def get_skycar_logs_with_fitler():

        limit = request.args.get('limit')
        elapsed_time=read_skycar_logs(limit)

        return {"times":elapsed_time}
   
    @app.route("/skycar-logs-with-filter-no-partition")
    def get_skycar_logs_with_fitler_no_partition():

        limit = request.args.get('limit')
        elapsed_time=read_skycar_logs_no_partition(limit)

        return {"times":elapsed_time}

    # def roll():
    #     return randint(1, dice)

    def generate_random_sc_logs()->str:
        return random.choice(sc_logs)
    
    def generate_fromatted_log()->dict:
        data = {
            "id": uuid.uuid1(),
            "sc_id":random.randint(1,10),
            "message":generate_random_sc_logs(),
            "created_at":int(time.time() * 1000)
        }
        return data
    
    def cassandra_insert_sc_logs_no_index(data:dict):
        session.execute(
            f"""
            INSERT INTO {db_name} (id, sc_id, message, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (data["id"],data["sc_id"], data["message"], data["created_at"]) #with miliseconds
        )
    
    def cassandra_read_from_db():
        query = f'SELECT * FROM {db_name}'
        statement = SimpleStatement(query, fetch_size=10)
        page_state = None
        results = session.execute(statement, paging_state=page_state)
        for user_row in results.current_rows:
            print(user_row)

        return results.paging_state

    def cassandra_query_from_db(limit:int):
        from_time = int((time.time() - (10 * 60)) * 1000)
        to_time = int(time.time() * 1000)
        sc_id = random.randint(1,10)
        rows = session.execute(
           f'SELECT sc_id,message,created_at \
            FROM {db_name} \
            WHERE sc_id = {sc_id} AND created_at >= {from_time} and CREATED_AT <= {to_time} \
            LIMIT {limit} \
            ').all()

        return rows
    
    def cassandra_query_from_db_no_partition(limit:int):
        from_time = int((time.time() - (10 * 60)) * 1000)
        to_time = int(time.time() * 1000)
        sc_id = random.randint(1,10)
        rows = session.execute(
           f'SELECT sc_id,message,created_at \
            FROM {db_name} \
            WHERE sc_id = {sc_id} AND created_at >= {from_time} and CREATED_AT <= {to_time} \
            LIMIT {limit} \
            ALLOW FILTERING \
            ').all()

        return rows
    
    def read_skycar_logs(limit:int):
        start_time = time.time()* 1000
        rows = cassandra_query_from_db(limit)
        end_time = time.time()* 1000
        elapsed_time = end_time - start_time
        print(f"READ time: {elapsed_time:.5f} milliseconds DB: {db_name}")
        stats_log.debug(f"READ WITH FILTER for {len(rows)} lines : {elapsed_time:.5f} milliseconds DB: {db_name}")

        for row in rows:
            created_time_unix = row.created_at
            timestamp_sec = created_time_unix // 1000
            timestamp_microsec = (created_time_unix % 1000) * 1000
            standard_time = datetime.datetime.fromtimestamp(timestamp_sec).replace(microsecond=timestamp_microsec)
            print(standard_time)
            print(row)

        return elapsed_time
    
    def read_skycar_logs_no_partition(limit:int):
        start_time = time.time()* 1000
        rows = cassandra_query_from_db_no_partition(limit)
        end_time = time.time()* 1000
        elapsed_time = end_time - start_time
        print(f"READ time: {elapsed_time:.5f} milliseconds DB: {db_name}")
        stats_log.debug(f"READ WITH FILTER for {len(rows)} lines : {elapsed_time:.5f} milliseconds DB: {db_name}")
        return elapsed_time

    def run(lines):
        counter = 0 
        total_insert_time = 0 
        insert_start_time = time.time()*1000
        while counter<lines:
            data = generate_fromatted_log()
            start_time = time.time()* 1000
            cassandra_insert_sc_logs_no_index(data)
            end_time = time.time()* 1000
            elapsed_time = end_time - start_time
            print(f"INSERTING ({counter}): {elapsed_time:.5f} milliseconds  DB: {db_name}")
            # stats_log.debug(f"INSERTING ({counter}): {elapsed_time:.5f} milliseconds DB: {db_name}")
            total_insert_time += elapsed_time
            
            counter += 1

        average_insert_time = total_insert_time/lines
        print(f"AVERAGE inserting time: {average_insert_time:.5f} milliseconds")
        stats_log.debug(f"AVERAGE INSERTING for {lines} lines: {average_insert_time:.5f} milliseconds DB: {db_name}")

        insert_end_time = time.time()*1000
        insert_elapsed_time = insert_end_time - insert_start_time
        print(f'Total WRITE time for {lines} lines : {insert_elapsed_time:.5f}')            
        stats_log.debug(f'TOTAL WRITE time for {lines} lines : {insert_elapsed_time:.5f}')            
    
    
    return app