import schedule
import time
from scripts.cron import main as cron


def main():
    # schedule.every(1).minutes.do(cron)   # TESTING
    schedule.every().day.at("01:00").do(cron)

    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()