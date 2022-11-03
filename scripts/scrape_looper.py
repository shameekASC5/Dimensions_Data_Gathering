#-----------------------------------------------------------------#
# scrape_looper.py
# Author: Shameek Hargrave
# Purpose: Based on cmd-line input, starts researchers or
# publications DIM.AI scrapers, maintaining persistant runtime by
# catching exceptions within the data scraping modules to restart
# data gathering from the point of failure.
#-----------------------------------------------------------------#
import os
import signal
import sys
import argparse
import time
from datetime import datetime
from pytz import timezone
import publications as publications_scraper
import researchers as researchers_scraper

#-----------------------------------------------------------------#
def set_cmd_args():
    parser = argparse.ArgumentParser(prog="scrape.py", description=
        'Maintainer program for researchers/publications API scraper.',
        allow_abbrev=False)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--researchers', action='store_true', help="set flag to true to run the researcher's data scraper.")
    group.add_argument('--publications', action='store_true', help="set flag to true to run the publication's data scraper.")
    return parser
#-----------------------------------------------------------------#
def get_last_args():
    """
    Reads from .txt files to find the last known id's pulled from API.
    """
    last_pub = ""
    last_researcher = ""
    with open("../config/lastpub_id.txt", "r") as file:
        last_pub = file.read()
    with open("../config/lastresearcher_id.txt", "r") as file:
        last_researcher = file.read()
    return last_pub, last_researcher
#-----------------------------------------------------------------#
def store_logs(log, timestamp):
    researchers_file = "../logs/server_logs/" + timestamp + ".txt"
    with open(researchers_file, "w+") as file:
        file.write(log)
#-----------------------------------------------------------------#
def main():
    # initialize the program
    parser = set_cmd_args()
    args = parser.parse_args()
    researcher_bot = args.researchers
    publications_bot = args.publications

    def term_received(n, stack):
        # do stuff here to close gracefully
        os.unlink(pidfile)
        sys.exit(0)
        return

    signal.signal(signal.SIGTERM, term_received)
    # create PID, define terminate behavior
    pid = str(os.getpid())
    # time.sleep(10)
    # print(pid)
    pidfile = "var/run/dim_ai_scrape.pid"
    # print(pidfile)
    if os.path.isfile(pidfile):
        print("%s already exists, exiting" % pidfile)
        sys.exit(127)
    else:
        # print("wrote pid")
        with open(pidfile, 'w') as file:
            file.write(pid)

    # begin logging
    begin = "...Beginning Data Scrape...\n"
    session_timestamp = time.strftime("%H_%M_%S",time.localtime())
    researchers_mode = "mode: researchers\n"
    pubs_mode = "mode: publications\n"
    line_br = "#----------------------------------------------------#\n"
    log = ""
    log += begin + session_timestamp + "\n" + line_br
    print("\n\n" + begin, file=sys.stderr)
    print(session_timestamp, file=sys.stderr)
    print(researchers_mode, file=sys.stderr) if researcher_bot else print(pubs_mode, file=sys.stderr)
    print(line_br)
    while True:
        # find next batch based on most recent id
        try:
            store_logs(log, session_timestamp)
            last_pub_id, last_researcher_id = get_last_args()
            # publications_scraper.source_pub_batch(last_pub_id) if publications_bot else researchers_scraper.source_researcher_batch(last_researcher_id)
            # print("Batch complete, logs saved.")
            if publications_bot:
                scrape_update = f"Scraping researchers from id: {last_pub_id}\n"
                log += scrape_update
                print(scrape_update, file=sys.stderr)
                publications_scraper.source_pub_batch(last_pub_id)
                store_logs(log, session_timestamp)
                log+= "\nEpoch complete, logs saved.\n"
                print("Epoch complete, logs saved.", file=sys.stderr)
            elif researcher_bot:
                scrape_update = f"Scraping researchers from id: {last_researcher_id}\n"
                log += scrape_update
                print(scrape_update, file=sys.stderr)
                researchers_scraper.source_researcher_batch(last_researcher_id)
                store_logs(log, session_timestamp)
                log+= "\nEpoch complete, logs saved.\n"
                print("Epoch complete, logs saved, restarting scrapers.", file=sys.stderr)
            # store_logs(researchers_log, publications_log)

        except Exception as err:
            store_logs(log, session_timestamp)
            error = f"ERROR: {err}\n"
            timestamp = "TIMESTAMP: " + time.strftime("%H_%M_%S",time.localtime()) + "\n"
            print(error, file=sys.stderr)
            print(timestamp, file=sys.stderr)
            # store_logs(researchers_log, publications_log)
            print("Restarting the scrapers.\n", file=sys.stderr)
            pass
            # handle control-c
            if isinstance(err, KeyboardInterrupt):
                sys.exit()
#-----------------------------------------------------------------#
if __name__ == "__main__":
    main()
#-----------------------------------------------------------------#

