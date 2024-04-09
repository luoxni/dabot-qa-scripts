
import requests
import json
import csv
import os
import logging
import pandas as pd
import ast
import numpy

input_path = '/Users/kishankumarravikumar/Dabot/testdata/QA-Data/SubSet1'
u_path="s3://dabot-testing-bucket/QA-Data/SubSet1/"
Url = "http://localhost:7001/api/i2a/v2/runi2a"
Target_file= "s3://dabot-testing-bucket/QA-Data/SubSet1/QA-Plan - Target_Schema.csv"

class testscript:
    
        
    header_added=False
        
    def Payload(filename):
        payload = json.dumps({
                                            "creatorId": 3,
                                            "source": {
                                                "path": str(u_path+filename),
                                                "type": "AWS S3",
                                                "isUpload": False,
                                                "connectionId": 2
                                            },
                                            "target": {
                                                "connectionId": 2,
                                                "type": "AWS S3",
                                                "path": Target_file ,#str(u_path+filename),
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
        




    def call_test(rootdir):
        
        for subdir, dirs, filepaths in os.walk(rootdir):
            for fp in filepaths:
                
                ext = os.path.splitext(fp)[-1].lower()
                filename =os.path.split(fp)[-1]
                
                if ext==".csv": 
                    #print(
                    print("\n", " next iteration ",str(u_path+filename))
                    output = requests.request("POST", Url, headers=testscript.headers, data=testscript.Payload(filename))
                    response= {output.text}
                    response_J= (output.json())
                    testscript.write_csv(response_J)

        
        return response_J
                   
                    

                

                
                
        
    def write_csv(response_J):
        csv_file_2 = '/Users/kishankumarravikumar/Dabot/output2.csv'
        csvf = open(csv_file_2, mode="a", encoding="utf-8")
        writer = csv.writer(csvf, delimiter=",")
        field_names = ['targetInfo', 'sourceInfo', 'mappings']
        row_values = []
        
        matches =response_J['mappings']['matches']
        
        
        for match in matches :
            match_values =[]
            index = matches.index(match)
            match_values.append(response_J["targetInfo"]["targetLocation"])
            match_values.append(response_J["sourceInfo"]["sourceLocation"])
            match_values.append(response_J["mappings"])
            field_names.append('column')#str(index))
            match_values.append(response_J["mappings"]['matches'][index]['displayName'])
            field_names.append('datatype')
            match_values.append(response_J["mappings"]['matches'][index]['datatype'])
            field_names.append('potentialMatch_C'+str(index))
            match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['potentialMatch'])
            field_names.append('sourceReference')
            match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['sourceReference'])
            field_names.append('confidence')
            match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['confidence'])
            field_names.append('matchType')
            match_values.append(response_J["mappings"]['matches'][index]['matchInfo'][0]['matchType'])
            new_row = row_values + match_values
            # print(new_row)
            if  testscript.header_added == False:
                writer.writerow(field_names)
                testscript.header_added=True
            writer.writerow(new_row)

            
            
            #print("The displayname of column",index,"is ",match["displayName"],"and its datatype:", match["datatype"])

            for match_infos in match["matchInfo"]:
                matchInfo_index = match["matchInfo"].index(match_infos)
                print(matchInfo_index)
                print("The displayname of column",index,"is ",match["displayName"],"and its datatype:", match["datatype"],"The potential match coloum is ",match_infos["potentialMatch"]," sourceReference: ",match_infos["sourceReference"],"with a confidence score :",match_infos["confidence"],"and match type",match_infos["matchType"])
            

            #print(index)
                    #print (match, '\t')
                
        # if  testscript.header_added == False:
        #         writer.writerow(field_names)
        #         testscript.header_added=True
        #writer.writerow(row_values)
            #writer.writerows([column])
                        

                    
                    #row_values.append(response_J['mappings']['matches'][1]['matchInfo'])
        


                

            

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="testlogfile", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")
    
    logging.info(testscript.call_test(input_path))

    #print(testscript1.call_test(input_path))
    
    
            
                
                
                
                
            



                







    




