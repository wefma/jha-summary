from logging import getLogger, StreamHandler, FileHandler, Formatter, INFO, ERROR


def init_logger():
    logger = getLogger("jha-summary")

    # set logger level so INFO messages are emitted
    logger.setLevel(INFO)

    ch = StreamHandler()
    ch.setLevel(INFO)
    ch_formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # avoid passing messages to the root logger (prevents duplicate logs)
    logger.propagate = False
