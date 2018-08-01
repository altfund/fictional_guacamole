import logging
logging.basicConfig(filename="/tmp/ws_error.log",
                    filemode="a",
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.CRITICAL)
