import logging
from logging import FileHandler, StreamHandler

def configure_logger():
    # Create a logger with the name of the current module (__name__)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # Set the logging level to INFO

    # FileHandler for writing logs to a file named 'my_log.log'
    handler_file = FileHandler('my_log.log')
    handler_file.setLevel(logging.INFO)  # Set the logging level for the file handler

    # StreamHandler for displaying logs in the terminal
    handler_console = StreamHandler()
    handler_console.setLevel(logging.INFO)  # Set the logging level for the console handler

    # Create a formatter to define the log message format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set the formatter for both handlers
    handler_file.setFormatter(formatter)
    handler_console.setFormatter(formatter)

    # Add both handlers to the logger
    logger.addHandler(handler_file)
    logger.addHandler(handler_console)

    return logger  # Return the configured logger
