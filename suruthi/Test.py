import csv
import json
import datetime
import time
import requests # type: ignore

def perform_post_request(url_template, host, version, payload):
    url = url_template.replace("{{host}}", host).replace("{{version}}", version)
    response = requests.post(url, json=payload)
    return response.json()

def main():
    input_csv_file = 'input.csv'

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_file = f'output_{timestamp}.csv'
    
    url_template = 'http://{{host}}:7001/api/i2a/{{version}}/runi2a'
    host = 'localhost'
    version = 'v2'
    
    with open(input_csv_file, 'r', encoding='utf-8-sig') as csvfile, open(output_csv_file, 'w', newline='') as outputfile:
        reader = csv.DictReader(csvfile)
  
        headers = [
            'targetInfo',
            'sourceInfo',
            'mappings',
            'column0',
            'datatype',
            'potentialMatch_C0',
            'sourceReference',
            'confidence',
            'matchType',
            'responseTime' 
        ]
        
        writer = csv.DictWriter(outputfile, fieldnames=headers)
        writer.writeheader()
        
        for row in reader:
            sourcepath = row['sourcepath']
            targetpath = row['targetpath']

            payload = {
                "creatorId": 3,
                "source": {
                    "path": sourcepath,
                    "type": "AWS S3",
                    "isUpload": False,
                    "connectionId": 2
                },
                "target": {
                    "connectionId": 2,
                    "type": "AWS S3",
                    "path": targetpath,
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
            }
            
            start_time = time.time()
            
            response_data = perform_post_request(url_template, host, version, payload)
            
            end_time = time.time()  
            response_time = end_time - start_time 
            
            if response_data:
                for match in response_data['mappings']['matches']:
                    for match_info in match['matchInfo']:
                        new_row = {
                            'targetInfo': json.dumps(response_data['targetInfo']),
                            'sourceInfo': json.dumps(response_data['sourceInfo']),
                            'mappings': json.dumps(response_data['mappings']),
                            'column0': match['column'],
                            'datatype': json.dumps(match['datatype']),
                            'potentialMatch_C0': match_info['potentialMatch'],
                            'sourceReference': match_info['sourceReference'],
                            'confidence': match_info['confidence'],
                            'matchType': match_info['matchType'],
                            'responseTime': response_time 
                        }
                        
                        writer.writerow(new_row)
                
    print(f"Completed! Results saved to {output_csv_file}")

if __name__ == "__main__":
    main()
