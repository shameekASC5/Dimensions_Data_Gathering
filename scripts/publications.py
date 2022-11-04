#-----------------------------------------------------------------#
# publications.py
# Author: Shameek Hargrave
# Purpose: Given the dimensions id of a publication, retrieves maximal
# batch data (~50,000) from API using paginated queries. Stores, a
# unique id, publication year, journal (optional), field/category, and
# a list of references (by id) in a csv file for each batch.
#-----------------------------------------------------------------#
# import argparse
import dimcli
import time
import csv
from datetime import datetime
from pytz import timezone
import random
import sys

# #-----------------------------------------------------------------#
# def set_cmd_args():
#     parser = argparse.ArgumentParser(description=
#         'Dimensions API scraper for publication data',
#         allow_abbrev=False)
#     parser.add_argument("start", metavar="start_pub",
#         help="the dimensions ID of the publication to begin retrieving batch data from.", type = str)
#     return parser
 # total_pubs/max_query ~2620 operations
    # 130,998,125 publications with strict limit of 50,000 returned in
    # a single query
    # total_pubs = 130998125
    # max_query = 50000
# #-----------------------------------------------------------------#
def set_last_pub(last_pub):
    """
    Stores the last id of most recent publication batch.
    """
    with open("../config/lastpub_id.txt", "w") as file:
        file.write(last_pub)
#-----------------------------------------------------------------#
# returns query data for batch of ~50000 publications ids greater
# than minima
def get_publications(dsl, min_pub_id):
    max_pub_id = "pub." + str(int(min_pub_id[4:])+ 50000)
    # print(max_pub_id)
    query = f"search publications where id>= \"{min_pub_id}\""
    query += f" and id< \"{max_pub_id}\" return "
    query += "publications[id+year+journal+category_for+reference_ids] "
    query += "sort by id asc "
    # print(query)
    data = dsl.query_iterative(query)
    time.sleep(1.5)
    if data.errors is not None:
        print("API endpoint error ", data.errors_string)
        return [None, "", ""]

    data = data.json["publications"]
    if len(data) > 0:
        first_pub_id = data[0]['id']
        last_pub_id = data[len(data)-1]["id"]
        set_last_pub(last_pub_id)
        return [data, first_pub_id, last_pub_id]
    return [None, "", ""]
#-----------------------------------------------------------------#
# write publications to data/first_pub-last_pub.csv file
def write_publications_to_csv(publications, first_pub_id, last_pub_id):
    fields = ["category_for", "id", "journal", "reference_ids", "year"]
    filepath = '../data/publications/' + first_pub_id  + "-" + last_pub_id + ".csv"
    # print(filepath)
    with open(filepath, 'w') as csvfile:
        # add new pub data
        writer = csv.DictWriter(csvfile, fieldnames = fields)
        writer.writeheader()
        writer.writerows(publications)
#-----------------------------------------------------------------#
def store_logs(publications_log, timestamp):
    publications_file = "../logs/publications_log/" + timestamp + ".txt"
    with open(publications_file, "w+") as file:
        file.write(publications_log)
#-----------------------------------------------------------------#
def source_pub_batch(start_pub):
    # logging
    runtime_log = "...Sourcing Publications Batch...\n"
    session_timestamp = time.strftime("%H_%M_%S",time.localtime())
    runtime_log += session_timestamp + "\n"
    runtime_log += f"starting id: {start_pub}\n"
    last_pub_id = "NOT FOUND"
    runtime_log += last_pub_id + "\n"
    iter = 0
    try:
        # find publications
        dimcli.login()
        dsl = dimcli.Dsl()
        max_pub_id = "pub.1152121493"
        pubs, first_pub_id, last_pub_id = get_publications(dsl, start_pub)
        write_publications_to_csv(pubs, first_pub_id, last_pub_id)
        runtime_log += f"Batch: {iter} processed!\n@" + time.strftime("%H_%M_%S",time.localtime()) + "\n"
        store_logs(runtime_log, session_timestamp)
        iter = 1
        # variable time delay
        delta = 5
        # main loop, continually source batches
        while last_pub_id <= max_pub_id:
            runtime_log += "\n#----------------------------------------------------#\n"
            runtime_log += f"Batch: {iter}\nwith time delay: {delta}\n"
            runtime_log += "Timestamp: " + time.strftime("%H_%M_%S",time.localtime()) + "\n"
            pubs, first_pub_id, last_pub_id = get_publications(dsl, last_pub_id)

            # publications parsed successfully
            runtime_log += f"last id: {last_pub_id}\n"
            current = int(last_pub_id.replace("pub.", ""))
            print(f"Publications overall progress: {str(current/1152121493)} % ({current}/1152121493)")
            runtime_log += f"Publications overall progress: {str(current/1152121493)} % ({current}/1152121493)\n"

            # store in file
            if pubs is not None:
                write_publications_to_csv(pubs, first_pub_id, last_pub_id)
                runtime_log += f"Batch sourced {str(len(pubs))} publications.\n"
                # runtime_log += f"Epoch has produced {str(iter*50000)} publications.\n"
                store_logs(runtime_log, session_timestamp)
                # print("last id" + last_pub_id)
                # first_pub_id = last_pub_id
                time.sleep(delta)
            delta += random.uniform(0, 0.1)
            iter +=1
            runtime_log += "#----------------------------------------------------#\n"

    except Exception as err:
        runtime_log += f"last id: {last_pub_id}\n"
        runtime_log += f"EXITING ON EXCEPTION: {err}\n"
        runtime_log += f"TYPE: {type(err)}"
        runtime_log += "#-----------------------------------------------------#\n"
        store_logs(runtime_log, session_timestamp)
    finally:
        store_logs(runtime_log, session_timestamp)


#-----------------------------------------------------------------#

# def main():
#     # initialize the program
#     parser = set_cmd_args()
#     args = parser.parse_args()
#     start_pub = args.start
#      asyncio.run(source_pub_batch(, start_pub))

# #-----------------------------------------------------------------#
# if __name__ == "__main__":
#     main()
# #-----------------------------------------------------------------#
