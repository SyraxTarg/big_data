import schedule
import time
from scripts.cron import main as cron
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    try:
        # schedule.every(1).minutes.do(cron)   # TESTING
        schedule.every().day.at("01:00").do(cron)

        while 1:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        logging.error(f'Une erreur est survenue lors de l\'exécution du job: {e}')

if __name__ == "__main__":
    main()