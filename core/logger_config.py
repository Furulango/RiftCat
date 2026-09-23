import logging

logging.basicConfig(
    filename="riftcat.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

def get_logger(nombre):
    return logging.getLogger(nombre)