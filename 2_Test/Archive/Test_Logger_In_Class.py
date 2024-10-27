from _utils import DataPipelineLogger
import time
import traceback

class TestLogger:
    def __init__(self):
        self.logger = DataPipelineLogger('TestLogger')

    def get_string(self):
        return "This is a test string"

    def print_results(self, string_input: str):
        print(string_input + str(1/0))

    def main(self):
        self.logger.write_to_log('******************************************')
        start_time = time.time()
        try:
            time.sleep(10)
            self.print_results(string_input=self.get_string())
        except Exception as e:
            error_trace = traceback.format_exc()
            self.logger.write_error_to_log(f"An error occurred: {e}\n{error_trace}")
            print(f"An error occurred: {e}")
        finally:
            end_time = time.time()
            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time, 60)
            self.logger.write_to_log(f"Main Function took {minutes:.0f} minutes and {seconds:.2f} seconds to run.")
            self.logger.write_to_log('******************************************')

if __name__ == '__main__':
    test_logger = TestLogger()
    test_logger.main()
