import json
import os

from datetime import datetime
from gcn_kafka import Consumer
from adsputils import load_config, setup_logging

proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging(
    "run.py",
    proj_home=proj_home,
    level=config.get("LOGGING_LEVEL", "INFO"),
    attach_stdout=config.get("LOG_STDOUT", False),
)

def consume_notices(subscription):
    client_id = config.get("GCN_NOTICE_ID", "no-id")
    client_secret = config.get("GCN_NOTICE_SECRET", "no-secret")
    logger.info("Launching Kafka consumer with id:%s, secret:%s" % (client_id, client_secret))
    consumer = Consumer(client_id=client_id, 
                        client_secret=client_secret)

    # Subscribe to topics and receive alerts
    consumer.subscribe(subscription)

    while True:
        for message in consumer.consume(timeout=1):
            if message.error():
                logger.error(message.error())
            topic = message.topic()
            offset = message.offset()
            value = message.value()
        
            log_msg = {"topic": topic,
                       "offset": offset}
            filename = "voevents/voevent_%s.xml" % str(datetime.now()).replace(' ','_')
            logger.info("GCN NOTICE: %s" % log_msg)
            try:
                with open(filename, "wb") as fe:
                    fe.write(value)
            except Exception as err:
                logger.warning("Couldn't write voevent xml: %s" % err)


def main():
    subs = config.get("VOEVENT_SUBSCRIPTIONS", [])
    try:
        consume_notices(subs)
    except Exception as err:
        logger.warning("oops: %s" % err)


if __name__ == '__main__':
    main()
