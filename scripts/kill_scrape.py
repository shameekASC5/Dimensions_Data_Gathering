#-----------------------------------------------------------------#
# kill_scrape.py
# Author: Shameek Hargrave
# Purpose: Terminates the /var/run/dim_ai_scrape.pid file containing
# the scrapelooper.py process.
#-----------------------------------------------------------------#
#!/usr/bin/python

import os, signal

#-----------------------------------------------------------------#
def main():
    pidfile = open('var/run/dim_ai_scrape.pid', 'r')
    pid = int(pidfile.read())
    os.kill(pid, signal.SIGTERM)
#-----------------------------------------------------------------#
if __name__ == "__main__":
    main()
#-----------------------------------------------------------------#
