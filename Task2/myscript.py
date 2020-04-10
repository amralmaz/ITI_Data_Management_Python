
#this for calculating the start time of execution
from datetime import datetime
startTime = datetime.now()

#defining the arguments
import argparse

parser = argparse.ArgumentParser(description='users patterns on websites')

parser.add_argument("dir", help = "Enter directory path")

parser.add_argument("-u", "--unix", action="store_true", dest="unixformat", default=False, help="keep the (time in) and (time out) in unix format")

args = parser.parse_args()

#start checking if there are any duplicates
checksums={}
duplicates=[]

# Import subprocess
from subprocess import PIPE, Popen
# Import os 
from os import listdir
# Get all files in that directory
from os.path import isfile, join
files = [item for item in listdir(args.dir) if (".json" in item)]

# Iterate over the list of files filenames
for filename in files:
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", args.dir+"/"+filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]

        # Append duplicate to a list if the checksum is found
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

if len(duplicates)>0:
    print(f"These Files are Duplicated: {duplicates}")

#start transformation 
import pandas as pd
from pandas.io.json import json_normalize 
import json

#create a function to convert the time the specific timezone
def timezone_change(time_column ):
    creation_timestamp = []
    for i, row in df.iterrows():
        stamp = pd.to_datetime(row[time_column], unit = 's').tz_localize(row['time_zone']).tz_convert('UTC')
        creation_timestamp.append(stamp)
    return creation_timestamp

#load the files and start transformation  
for filename in files:
    if filename not in duplicates:
        records = [json.loads(line) for line in open(filename)]
        #to set a parent key for the list
        myjson = {'myrecords': records}
        
        #to transform the dictionary into dataframe
        df = json_normalize(myjson['myrecords'])
        
        #to select only the columns we need 
        df=df[['a','tz','r', 'u', 't', 'hc', 'cy', 'll']]
        	
        #to cut the web_browser from column 'a'
        new = df["a"].str.split("/", n = 1, expand = True) # Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4...
        df['web_browser']=new[0]
        
        #to cut the operating_sytem from column 'a'
        new = df["a"].str.split("(", n = 1, expand = True) 
        new= new[1].str.split(" ", n = 1, expand = True)
        new= new[0].str.split(";", n = 1, expand = True)
        df['operating_sys']=new[0] 
        
        #to get the website name 'www.example.com' out of column 'r' 
        new = df["r"].str.split("//", n = 1, expand = True)    # http://www.nasa.gov/mission_pages/nustar/main/...
        new= new[1].str.split("/", n = 1, expand = True)
        df['from_url']=new[0]
        
        #to get the website name 'www.example.com' out of column 'u'
        new = df["u"].str.split("//", n = 1, expand = True) 
        new= new[1].str.split("/", n = 1, expand = True)
        df['to_url']=new[0]
        
        #create new column just to be in the same sequence 
        df['city']=df['cy']
        
        #to get the longitude in new column out of column'll'
        df['longitude']=df['ll'].str[0]
        
        #to get the latitude in new column out of column'll'
        df['latitude']=df['ll'].str[1]
        
        #to get the time zone 
        df['time_zone']=df['tz']

        #drop null values
        df = df.dropna()
        
        #check whether the user enters '-u' which will be true if the user enter it
        if args.unixformat:
            #Timestamp when the user start using the website in unix format
            df['time_in']=df['t']
            
            #Timestamp when the user start using the website in unix format
            df['time_out']=df['hc']
        else:    
            #Timestamp when the user start using the website in datetime format
            df['time_in']=timezone_change("t")
            
            #to get the Timestamp when user exit the website in datetime format
            df['time_out']=timezone_change("hc")
        
	#drop all the old columns 
        df=df.drop(df.columns[df.columns.get_loc('a'):df.columns.get_loc('ll')+1], axis=1)
        
	#print number of rows transformed
        print('there are {} rows transformed from file:{}'.format(df.shape[0], args.dir+"/"+filename)) 
        
        #to save the files
        outputfile=filename.replace('.json',' ')
        df.to_csv('/home/almaz/Desktop/ITI/Task2/target/'+outputfile+'.csv')
	
#print the execution time of the script
execution_time= (datetime.now() - startTime)
print("The total execution time of the script is: {}".format(execution_time))



           
        
          
        
         
           
          
             
           
