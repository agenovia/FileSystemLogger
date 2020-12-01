import logging

LEVEL = logging.DEBUG


def start_logging():
    logging.basicConfig(format='[%(asctime)s.%(msecs)03d] [%(filename)s:%(lineno)s - %(funcName)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=LEVEL
                        )
