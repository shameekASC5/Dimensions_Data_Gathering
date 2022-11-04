#-----------------------------------------------------------------#
# researchers.py
# Author: Shameek Hargrave
# Purpose: Given the dimensions id of a researcher to begin searching /
# from, retrieves maximal batch data (~50,000) from API using paginated
# queries. Write csv files containing each researcher's name, dimension.
# ai unique identifier, and a list of all his/her papers (by id).
#-----------------------------------------------------------------#
# import argparse
import dimcli, time, csv
from datetime import datetime
from pytz import timezone
import random
import sys
# import time
# import csv

# #-----------------------------------------------------------------#
# def set_cmd_args():
#     parser = argparse.ArgumentParser(description=
#         'Dimensions API scraper for researcher data',
#         allow_abbrev=False)
#     parser.add_argument("start", metavar="start_id",
#         help="the dimensions ID of the researcher to begin retrieving #     batch data from.", type = str)
#     return parser
#-----------------------------------------------------------------#
# returns list of publications corresponding to each researchers_id
# def find_publications(dsl, researchers_ids):
#     # loop through researchers to find publications
#     for researcher in researchers_ids:
#         dim_id = researcher["id"]
#         cmd = """search publications where researchers in [\" """ + dim_id + """\"] return publications[id] sort by id asc"""
#         # print(cmd)
#         data = dsl.query_iterative(cmd)
#         researcher_pubs_id = data.json["publications"]
#         # add to csv
#         researcher["publication_ids"] = researcher_pubs_id
#         time.sleep(2)
# #     return researchers_ids
#-----------------------------------------------------------------#
def store_logs(researchers_log, timestamp):
    researchers_file = "../logs/researchers_log/" + timestamp + ".txt"
    with open(researchers_file, "w+") as file:
        file.write(researchers_log)
#-----------------------------------------------------------------#
def write_researchers_to_csv(researchers, first_id, last_id):
    """
    write to data/researcher_ids.csv file
    """
    fields = ["first_name", "id", "last_name", "publication_ids"]
    filepath = '../data/researchers/' + first_id  + "-" + last_id + ".csv"

    with open(filepath, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = fields)
        writer.writeheader()
        writer.writerows(researchers)
#-----------------------------------------------------------------#
def set_last_researcher(last_researcher):
    """
    Stores the last id of most recent researcher batch.
    """
    with open("../config/lastresearcher_id.txt", "w") as file:
        file.write(last_researcher)
#-----------------------------------------------------------------#
def get_researchers_ids(dsl, min_id):
    # returns jSON data for up to 50,000 researcher's ids
    query = f"search researchers where id>=\"{min_id}\" return "
    query += "researchers[first_name+last_name+id] sort by id asc"
    data = dsl.query_iterative(query)
    time.sleep(1.5)
    if data.errors is not None:
        print("API endpoint error ", data.errors_string)
        return [None, "", ""]

    data = data.json["researchers"]
    if len(data) > 0:
        first_id = data[0]['id']
        last_id = data[len(data)-1]['id']
        set_last_researcher(last_id)
        return [data, first_id, last_id]

    # iter = 1
    # # remove publications from search for now
    # for researcher in data:
    #     dim_id = researcher["id"]
    #     print(f"Iteration: {iter}/50,000 ({int(iter/50000)}%)")
    #     cmd = """search publications where researchers in [\" """ + dim_id + """\"] return publications[id] sort by id asc"""
    #     # print(cmd)
    #     associated_pubs = dsl.query(cmd, verbose=False).json["publications"]
    #     # add to csv
    #     researcher["publication_ids"] = associated_pubs
    #     iter+=1
    #     time.sleep(2)
    return [None, "", ""]
#-----------------------------------------------------------------#
def source_researcher_batch(min_id):
    # logging
    runtime_log = "...Sourcing Researchers Batch...\n"
    session_timestamp = time.strftime("%H_%M_%S",time.localtime())
    runtime_log += session_timestamp + "\n"
    runtime_log += "starting id: " + str(min_id) + "\n"
    iter = 0
    last_id = "NOT FOUND"
    runtime_log += last_id + "\n"
    store_logs(runtime_log, session_timestamp)

    try:
        # total_researchers = 50764112
        # max_query = 50000
        # total_researchers /max_query ~1015 operations
        # find researchers
        dimcli.login()
        dsl = dimcli.Dsl()
        researchers, first_id, last_id = get_researchers_ids(dsl, min_id)
        write_researchers_to_csv(researchers, first_id, last_id)
        runtime_log += f"Batch: {iter} processed!\n@" + time.strftime("%H_%M_%S",time.localtime()) + "\n"
        store_logs(runtime_log, session_timestamp)
        # variable time delay
        delta = 5
        iter = 1
        # main loop, continually source batches of researcherss
        max_id = "ur.07777777753.45"
        while last_id <= max_id:
            runtime_log += "\n#----------------------------------------------------#\n"
            runtime_log += f"Batch: {iter}\nwith time delay: {delta}\n"
            runtime_log += "Timestamp: " + time.strftime("%H_%M_%S",time.localtime()) + "\n"
            researchers_ids_and_pubs, first_id, last_id =       get_researchers_ids(dsl, last_id)
            runtime_log += f"last id: {last_id}\n"
            if researchers_ids_and_pubs is not None:
                write_researchers_to_csv(researchers_ids_and_pubs, first_id, last_id)
                runtime_log += f"Batch sourced {str(len(researchers_ids_and_pubs))} researchers.\n"
                store_logs(runtime_log, session_timestamp)
                time.sleep(delta)
                runtime_log += "#-----------------------------------------------------#\n"
            delta += random.uniform(0, 0.1)
            iter +=1

    except Exception as err:
        runtime_log += f"last id: {last_id}\n"
        runtime_log += f"EXITING ON EXCEPTION: {err}\n"
        runtime_log += f"TYPE: {type(err)}"
        runtime_log += "#----------------------------------------------------#\n"
        store_logs(runtime_log, session_timestamp)

    finally:
        store_logs(runtime_log, session_timestamp)
# #-----------------------------------------------------------------#
# def main():
#     # initialize the program
#     parser = set_cmd_args()
#     args = parser.parse_args()
#     start_id = args.start
#     # find researchers batch based on starting researcher id
#      source_researcher_batch(start_id)
# #-----------------------------------------------------------------#
# if __name__ == "__main__":
#     main()
# #-----------------------------------------------------------------#
