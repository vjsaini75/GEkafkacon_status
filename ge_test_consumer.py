import io
import datetime
import logging
import kafka
import avro
from gedict import Quality
from logging.handlers import TimedRotatingFileHandler
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

kafka_addr = '10.108.21.32:9093'
kafka_topic = 'scada-status'
avro_format = '/app/scadastatus.avsc'
out_file = 'status.log'

# avro decoder
def avro_handler(format):
    print(format)
    schema = avro.schema.parse(open(format, "rb").read())
    reader = avro.io.DatumReader(schema)
    return reader

# kafka consumer
def consumer_latest(topic,broker_addr):
    consumer = kafka.KafkaConsumer(
        topic,
        bootstrap_servers=[broker_addr],
        auto_offset_reset='latest',
        group_id='vijay01')
    if (consumer !=0):
        print ("consumer created")
    return consumer    

# file handler
def init_log(file):
    print ("init log")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    try:
        handler = TimedRotatingFileHandler(file, when='d', interval=1, backupCount=3)
        logger.addHandler(handler)
        print("log file handler ready")
    except:
        print("error in open file")
    return logger

def timestamp2timestr(stamp):
    if (stamp != 0):
        timestamp = datetime.datetime.fromtimestamp(stamp/1000)
        timestring = str(timestamp.strftime('%Y-%m-%d %H:%M:%S.%f'))
    else:
        timestring = "no time"
    return timestring
     


def read_data(consumer_avro,reader_format_handler,logformat): 
    for message in consumer_avro:
        message_key_str = str(message.key)
        message_val = message.value
        # print (message.key)
        # id from ABS.ESTN.RTU.S327
        if (message_key_str.find('0cbdec0a') != -1):
            bytes_reader = io.BytesIO(message_val)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            # decoding in bytes
            try:
                js_obj = reader_format_handler.read(decoder)
                # decoding timestamp
                fld_time = timestamp2timestr(js_obj['time'])
                prod_time = timestamp2timestr(message.timestamp)
                sys_time = str(datetime.datetime.now())
                # decoding quality dict
                try:
                    Qual = Quality[str(js_obj['quality1'])]
                except:
                    Qual = str(js_obj['quality1'])

                print("Id = " + str(js_obj['id']) +
                    " Name = " + js_obj['name'] +
                    " Value = " + str(js_obj['value']) +
                    " Quality =" + Qual +
                    " FLD_Time =" + fld_time + 
                    " Prod_time = " + fld_time +
                    " sys_time = " + sys_time)
                    
                data_line = ("Id = " + str(js_obj['id']) + 
                        " Name = " + js_obj['name'] +
                        " Value = " + str(js_obj['value']) +
                        " Quality =" + Qual +
                        " FLD_Time =" + fld_time  +
                        " Prod_time = " + prod_time +
                        " sys_time = " + sys_time)
                logformat.info(data_line)
                consumer_avro.commit()
            except IOError:
                logformat.info("error data")
                print("error data")


if __name__ == "__main__":
    print(__name__)
    log = init_log(out_file)
    #h_avro = avro_handler(avro_format)
    schema = avro.schema.parse(open(avro_format, "rb").read())
    reader = avro.io.DatumReader(schema)
    h_consumer = consumer_latest(kafka_topic,kafka_addr)
    read_data (h_consumer,reader,log)
    