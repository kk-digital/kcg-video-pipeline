import logging

# Configure logging levels
logging.basicConfig(level=logging.DEBUG)

# Create a logger object
logger = logging.getLogger(name=__name__)

# Create console handler and set level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create file handler and set level to ERROR
file_handler = logging.FileHandler(filename="error.log", mode='a')
file_handler.setLevel(level=logging.WARNING)

# Create formatter
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")

# Add formatter to handlers
file_handler.setFormatter(fmt=formatter)
console_handler.setFormatter(fmt=formatter)

# Add handlers to logger
logger.addHandler(hdlr=console_handler)
logger.addHandler(hdlr=file_handler)

# disable default console handler
logger.propagate = False