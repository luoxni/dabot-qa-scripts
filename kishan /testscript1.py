
import requests
import json
import csv
import os
import logging
import pandas as pd
import ast
import numpy
import time 

input_path = '/Users/kishankumarravikumar/Dabot/testdata/QA-Data/'
Url = "http://localhost:7001/api/i2a/v_ex/runi2a"

       
header_added = False
Targets = []
Sources =[]
    
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
    
    load_test_file(rootdir)

    for  Target in Targets:
        Tf = Target
        
        for Source in Sources:
            Sf = Source
            

        #print("\n", " next iteration ",Target , "\n"))
            start_time = time.time()
            print(Tf, Sf)
            output = requests.request("POST", Url, headers=headers, data=Payload(Tf,Sf))
            end_time = time.time()
            response= {output.text}
            response_time = end_time - start_time 
            response_J= (output.json())
            write_csv(response_J ,response_time)

    
    return response_J
                        
    
def write_csv(response_J,response_time):
    global header_added
    csv_file_2 = '/Users/kishankumarravikumar/Dabot/output2.csv'
    csvf = open(csv_file_2, mode="a", encoding="utf-8")
    writer = csv.writer(csvf, delimiter=",")
    field_names = ['targetInfo', 'sourceInfo', 'mappings', 'coloum','datatype','potentialMatch','Sourcereference', 'confidence','potentialMatch_datatype', 'matchtype','respond_time']
    row_values = []
    
    matches = response_J['mappings']['matches']
    
    
    for match in matches :
        match_values =[]
        index = matches.index(match)
        match_values.append(response_J["targetInfo"]["targetLocation"])
        match_values.append(response_J["sourceInfo"]["sourceLocation"])
        match_values.append(response_J["mappings"])
        #field_names.append('column')#str(index))
        match_values.append(response_J["mappings"]['matches'][index]['displayName'])
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
    
    
            
                
                
                
                
            



                







    




