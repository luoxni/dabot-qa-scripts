
import requests
import json
import csv
import os
import logging
import pandas as pd
import ast
import numpy
import time

header_added = False
header_added_for_failed=False
input_path = '/Users/kishankumarravikumar/Dabot/testdata/QA-Data'
Url = "http://localhost:7001/api/i2a/v_ex/runi2a"
Targets = []
Sources =[]
runcount=1
temp_run_count=0

def Payload(Target,source):
    payload = json.dumps({
                            "creatorId": 3,
                            "source": {
                                "path": source,#str(u_path+filename),
                                "type": "AWS S3",
                                "isUpload": False,
                                "connectionId": 2
                            },
                            "target": {
                                "connectionId": 2,
                                "type": "AWS S3",
                                "path": Target,#Target_file ,#str(u_path+filename),
                                "isUpload": False,
                                "fileName": None
                            },
                            "botId": 45,
                            "preview": True,
                            "useSmarthub": True,
                            "useConnections": True,
                            "saveSmarthub": True,
                            "sourcePreprocessHash": "K_0qZi6FWYM",
                            "targetPreprocessHash": "LTdWcoNrFXI",
                            "connectionReadLimit": 2500,
                            "setAILevel": 0
                            })
    return payload

headers = {
                'x-correlation-id': 'testcor1',
                'x-session-id': 'session1',
                'Authorization': 'Bearer: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MiwiZW1haWwiOiJrYXJ0aGlrQGRhYm90LmFpIiwibmFtZSI6IkthcnRoaWsgS2FubmFuIiwiaWF0IjoxNzA1OTU2NDMyLCJleHAiOjE3MDYwNDI4MzJ9.vp26KY9cglYOqsmZrx5In4Z2llHArPGqkjBVflya9ew',
                'Content-Type': 'application/json'
                }

def load_test_file(rootdir):

        for subdir, dirs, filepaths in os.walk(rootdir):
            for fp in filepaths:

                ext = os.path.splitext(fp)[-1].lower()
                filename =os.path.split(fp)[-1]

                if ext==".csv":
                    expected_subdir = subdir.replace("/Users/kishankumarravikumar/Dabot/testdata/", "s3://dabot-testing-bucket/")
                    if "Target" in fp:
                        Targets.append(str(os.path.join(expected_subdir, fp)))
                    else:
                        Sources.append(str(os.path.join(expected_subdir, fp)))

def call_test(rootdir):
    global runcount

    load_test_file(rootdir)

    for  Target in Targets:
        Tf = Target
        Tf_path = os.path.dirname(Tf)

        tf_run_count = 0
        for Source in Sources:
            Sf = Source
            Sf_path = os.path.dirname(Sf)
            if  Tf_path == Sf_path :
                #print("\n", " next iteration ",Target , "\n"))
                start_time = time.time()
                print(Tf, Sf ,"current run ",runcount)
                runcount+=1
                output = requests.request("POST", Url, headers=headers, data=Payload(Tf,Sf))
                end_time = time.time()
                response= {output.text}
                response_time = end_time - start_time
                response_J= (output.json())
                if response_J['success']==True :
                    write_csv(response_J ,response_time, runcount)
                else :
                    write_csv_failed(Tf,Sf)
    return response_J ,runcount

def write_csv_failed(Tf,Sf): 
    global header_added_for_failed
    csv_failed = '/Users/kishankumarravikumar/Dabot/failed_output.csv'
    csv_f =open( csv_failed, mode='a', encoding="utf-8")
    writer = csv.writer(csv_f ,delimiter=",")
    field_names = ['targetInfo', 'source info']
    row_values = []
    if  header_added_for_failed == False:
            writer.writerow(field_names)
            header_added_for_failed=True
    row_values.append(Tf)
    row_values.append(Sf)
    writer.writerow(row_values)
    
    
def write_csv(response_J,response_time,runcount):
    global header_added
    global temp_run_count
    csv_file_2 = '/Users/kishankumarravikumar/Dabot/output2.csv'
    csv_s = open(csv_file_2, mode="a", encoding="utf-8")
    writer = csv.writer(csv_s, delimiter=",")
    field_names = ['runcount','targetInfo', 'sourceInfo', 'coloum','datatype','potentialMatch','Sourcereference', 'confidence','potentialMatch_datatype', 'matchtype','respond_time']
    row_values = []

    matches = response_J['mappings']['matches']

    for match in matches :
        match_values =[]
        index = matches.index(match)
        if temp_run_count!=runcount:
            match_values.append(runcount)
            temp_run_count= runcount
        else:
            match_values.append(" ")
        
        match_values.append(response_J["targetInfo"]["targetLocation"])
        match_values.append(response_J["sourceInfo"]["sourceLocation"])
        target_index =(response_J["mappings"]['matches'][index]['targetPosition'])
        match_values.append(response_J["sourceInfo"])  
        #field_names.append('column')#str(index))
        match_values.append(response_J["mappings"]['matches'][index]['displayName'])
        target_index =(response_J["mappings"]['matches'][index]['targetPosition'])
        match_values.append(response_J["targetInfo"]["columnData"][source_index])
        source_index =(response_J["mappings"]['matches'][index]['sourcePosition'])
        match_values.append(response_J["sourceInfo"]["columnData"][source_index])
        #field_names.append('datatype')
        match_values.append(response_J["mappings"]['matches'][index]['datatype'])
        #field_names.append('potentialMatch')#+str(index))
        match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['potentialMatch'])
        #field_names.append('sourceReference')
        match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['sourceReference'])
        #field_names.append('confidence')
        match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['confidence'])
        #field_names.append('potentialMatch_datatype')
        match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['datatype'])
        #field_names.append('matchType')
        match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['matchType'])
        #field_names.append('response_time')
        match_values.append(response_time)

        new_row = row_values + match_values
        # print(new_row)
        if  header_added == False:
            writer.writerow(field_names)
            header_added=True
        writer.writerow(new_row)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="testlogfile", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

    logging.info(call_test(input_path))
    call_test(input_path)
