from dotenv import load_dotenv
import requests
import json
import pandas as pd
import csv
import shutil
import os
from pathlib import Path

class CloudCard:
    def __init__(self, username, password, base_url):
        self.username= username
        self.password= password
        self.path_output = ''
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
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            data = response.json()
            jwt = data['access_token']
            self.access_token+=jwt
            print('getting token...')
        except:
            raise Exception("Error getting token")
    def get_photo_submissions(self):
        url = "{}/photos".format(self.base_url)
        payload={}
        headers = {
        'X-Auth-Token': self.access_token
        }
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            response_json=response.json()
            self.raw_response.append(response_json)
            print('getting photo submissions...')
        except:
            raise Exception("Error fetching photo submissions")
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
                    person_shape.append(raw_response["dateCreated"][counter])
                    person_shape.append(raw_response["personHasApprovedPhoto"][counter])
                    person_shape.append(raw_response["status"][counter])
                    person_shape.append(raw_response["publicKey"][counter])
                    if (len(person["additionalPhotos"]) > 0):
                        person_shape.append(person["additionalPhotos"][0]["person"]["dateCreated"])
                        person_shape.append(person["additionalPhotos"][0]["person"]["dateTermsAccepted"])
                        person_shape.append(person["additionalPhotos"][0]["person"]["lastUpdated"])
                    else:
                        person_shape.append('')
                        person_shape.append('')
                        person_shape.append('')
                    data.append(person_shape)
                    #print(counter)
                    counter+=1                    
        self.transformed_data.append(data)
        print('transforming response...')

    def create_file(self):
        output_folder = Path("//file02/Operations/Integrations/cc_output")
        self.path_output=str(output_folder)
        headers = ['source_id', 'email', 'identifier', 'dateCreated', 'has_approved_photo', 'status', 'public_key', 'additional_photos_date_created', 'dateTermsAccepted', 'last_updated']
        try:
            with open('cc_photos.csv','w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f) 
                writer.writerow(headers)
                writer.writerows(self.transformed_data[0])
                self.file_name='cc_photos.csv'
        except:
            with open('cc_error.csv','w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f) 
                writer.writerow(headers)
                self.file_name='cc_error.csv'
        finally:
            f.close()
            shutil.copy(self.file_name,output_folder)
            print('Finished writing and creating {}. Check folder path for csv.'.format(self.file_name)) 

load_dotenv()
username=os.getenv('CLOUD_CARD_USERNAME')
password=os.getenv('CLOUD_CARD_PASSWORD')
base=os.getenv('CLOUD_CARD_BASE_URL')

instance = CloudCard(username, password, base)
instance.get_token()
instance.get_photo_submissions()
instance.transform_data()
instance.create_file()


