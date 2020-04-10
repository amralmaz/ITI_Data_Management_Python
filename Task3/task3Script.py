

import sqlalchemy as db
import psycopg2

# create connection with the database
con = db.create_engine('postgresql://amr:root@localhost/data_management')

import pandas as pd
#the query that select the new data that is not exist in scored_diabetes table
query=""" select * from unscored_diabetes EXCEPT select pregnancies ,
  glucose ,
  bloodpressure ,
  skinthickness ,
  insulin ,
  bmi,
  diabetespedigreefunction ,
  age 
  from scored_diabetes;"""

unscored_diabetes=pd.read_sql(query,con)

from keras.models import model_from_json

#load the json file that has the architecture of the model
json_file = open('/home/almaz/Desktop/ITI/Task3/model.json', 'r')
model_json = json_file.read()
json_file.close()
prediction_model = model_from_json(model_json)

#load the weights from 'model.h5' into the model
prediction_model.load_weights('/home/almaz/Desktop/ITI/Task3/model.h5')

import numpy as np
#we have to convert the dataset into array to pass it to the model 
dataset_array=unscored_diabetes.to_numpy()
#use the prediction_model to predict the the outcome column
outcome_results=prediction_model.predict(dataset_array)  #array([[0.26125655],[0.51930404],....])
    
#to approximate the outcome_results to be (0 or 1) 
outcome=[]
for i in outcome_results:
    for j in i:
        if j>=0.5:
           outcome.append(1) 
        else: outcome.append(0)  

#to create the scored_diabetes dataframe with the predicted column "outcome"
scored_diabetes=unscored_diabetes.copy()
scored_diabetes['outcome']=outcome

#to save the scored_diabetes dataframe to the database
scored_diabetes.to_sql(name = 'scored_diabetes',                       
                con=con,                                        
                schema = 'public',index = False,                                  
                if_exists='append')


#to schedule the script
#echo ("0**** python /home/almaz/Desktop/ITI/Task3/task3script.py") > /tmp/mycrontab
#crontab /tmp/mycrontab

