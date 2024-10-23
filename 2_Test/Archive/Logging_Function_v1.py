import logging
import time

class DataPipelineLogger:
    def __init__(self, log_file_name):
        self.log_file_name = "Logging/" + log_file_name + ".log"
        # Configure logging
        logging.basicConfig(
            filename=self.log_file_name,
            level=logging.INFO,  # Change to ERROR if you only want to log errors
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def run_with_logging(self, func):
        start_time = time.time()
        try:
            logging.info(f"{func.__name__} started.")
            func()
            logging.info(f"{func.__name__} completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred in {func.__name__}", exc_info=True)
        finally:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info(f"{func.__name__} took {elapsed_time:.2f} seconds to run.")

    def write_to_log(self, input_string: str):
        logging.info(input_string)

# Example function
def main_function():
    # Your main script logic here
    logging.info("Main function logic goes here.")
    x = 1 / 0  # Example code that could raise an error

if __name__ == '__main__':
    logger = DataPipelineLogger('your_script_v2')
    logger.run_with_logging(main_function)
