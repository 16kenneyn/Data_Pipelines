import logging
import sqlalchemy
import pymssql

logging.basicConfig(
    filename='3_Logging/script.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        # Your script code here
        logging.info("Script started.")
        # Example command that might fail
        x = 1 / 0  # Will raise ZeroDivisionError
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)

if __name__ == '__main__':
    main()
