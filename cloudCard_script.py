from dotenv import load_dotenv
from datetime import datetime
import requests
import json
import pandas as pd
import csv
import shutil
import os
import time

class CloudCard:
    def __init__(self, username, password, path_output, base_url):
        self.username= username
        self.password= password
        self.path_output = path_output
        self.base_url=base_url
        self.access_token=''
        self.raw_response=[]
        self.transformed_data=[]
        self.file_name='cc_photos_output_'
    def get_token(self):
        print('Task starting up!')
        url = "{}/login".format(self.base_url)
        login = {'username':self.username,'password':self.password}
        payload=json.dumps(login)
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        jwt = data['access_token']
        self.access_token+=jwt
        print('getting token...')
    def get_photo_submissions(self):
        url = "{}/photos".format(self.base_url)
        payload={}
        headers = {
        'X-Auth-Token': self.access_token
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json=response.json()
        self.raw_response.append(response_json)
        print('getting photo submissions...')
    def transform_data(self):
        raw_response = pd.DataFrame(self.raw_response[0])
        data=[]
        for column in raw_response.columns:
            counter=0
            if column=="person":
                for person in raw_response["person"]:
                    person_shape=[]
                    for index in person:
                        if index == "email":
                            person_shape.append(person[index])
                        if index == "identifier":
                            person_shape.append(person[index])
                        if index == "id":
                            person_shape.append(person[index])
                    person_shape.append(raw_response["personHasApprovedPhoto"][counter])
                    person_shape.append(raw_response["status"][counter])
                    person_shape.append(raw_response["publicKey"][counter]    )
                    data.append(person_shape)
                    #print(counter)
                    counter+=1                    
        self.transformed_data.append(data)
        print('transforming response...')

    def create_file(self):
        timestamp = time.time()
        # convert to datetime
        date_time = datetime.fromtimestamp(timestamp)

        # convert timestamp to string in dd-mm-yyyy HH:MM:SS
        str_date_time = date_time.strftime("%d-%m-%Y_%H-%M-%S")
        self.file_name+=str_date_time
        headers = ['source_id', 'email', 'identifier', 'has_approved_photo', 'status', 'public_key']
        with open('{}.csv'.format(self.file_name),'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(self.transformed_data[0])
            f.close()
        shutil.copy2('{}.csv'.format(self.file_name),self.path_output)
        print('Finished writing and creating {}. Check folder path for csv.'.format(self.file_name)) 

load_dotenv()
username=os.getenv('CLOUD_CARD_USERNAME')
password=os.getenv('CLOUD_CARD_PASSWORD')
path=os.getenv('INTEGRATIONS_FILE_PATH')
base=os.getenv('CLOUD_CARD_BASE_URL')

instance = CloudCard(username, password, path, base)
instance.get_token()
instance.get_photo_submissions()
instance.transform_data()
instance.create_file()