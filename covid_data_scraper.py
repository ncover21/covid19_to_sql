from datetime import timedelta, date
import requests, logging, os, traceback
import sqlalchemy as sa
#import pymysql
import pandas as pd
import json
#import MySQLdb
# pip install PyMySQL
# pip install python-dotenv


ENV_TYPE = 'dev'
WRITE_LOCAL = False
WRITE_DB = True

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

def load_to_sql(fn_date, url, con):
    try:
        df = pd.read_csv(url)
        df.to_sql(name='covid_{}'.format(fn_date.replace("-","_")), if_exists='replace', con=con)
        logging.debug("Sucessfuly Loaded Date: {}".format(fn_date))
    except:
        logging.error("Could not insert date: {} into SQL db".format(fn_date))
        if ENV_TYPE == 'dev' or ENV_TYPE == 'test':
            traceback.print_exc()
        return False
    
    return True

def upload_zip_codes(csv_file):
    try:
        #dialect+driver://username:password@host:port/database
        logging.debug("Uploading Zip Codes...")
        engine_string = 'mysql://{0}:{1}@{2}:{3}/{4}'.format(os.getenv("SQL_USER"),os.getenv("SQL_PASSWORD"),os.getenv("SQL_HOST"), os.getenv("SQL_PORT"),os.getenv("SQL_DB"))
        con = sa.create_engine(engine_string)
        logging.debug("Established Connection")
        df = pd.read_csv(csv_file)
        df.to_sql(name='zipcodes', if_exists='replace', con=con)
        logging.debug("Finished Uploading")
    except:
        logging.error("Could not insert data into SQL db")
        if ENV_TYPE == 'dev' or ENV_TYPE == 'test':
            traceback.print_exc()
        return False
    logging.info("Sucessfully Uploaded Zip Codes")
    return True

def main(date_range):
    endpoint = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
    sucessful_uploads = 0
    #01-23-2020.csv
    # The size of each step in days
    if date_range == -1:
        start_date = date(2020, 1, 22)
    else:
        start_date = date.today() - timedelta(days=date_range)
    
    end_date = date.today()
    logging.debug("Start Date: {} | End Date: {}".format(start_date, end_date))

    if WRITE_DB:
        #dialect+driver://username:password@host:port/database
        logging.info("Establishing DB Connection")
        engine_string = 'mysql://{0}:{1}@{2}:{3}/{4}'.format(os.getenv("SQL_USER"),os.getenv("SQL_PASSWORD"),os.getenv("SQL_HOST"), os.getenv("SQL_PORT"),os.getenv("SQL_DB"))
        connection = sa.create_engine(engine_string)

    for single_date in daterange(start_date, end_date):
        current_date = single_date.strftime("%m-%d-%Y")

        logging.debug("Processing Date: {}".format(current_date))
        url = "{}{}.csv".format(endpoint, current_date)
        
        if WRITE_LOCAL:
            req = requests.get(url)
            url_content = req.content
            if req.status_code == 200:
                csv_file = open('dbs/{}.csv'.format(current_date), 'wb')
                csv_file.write(url_content)
                csv_file.close()
                logging.debug("\tDone")
            else:
                logging.error("\tFile Not Found")
                logging.error("\tStatus Code: {}".format(req.status_code))
        if WRITE_DB:
            if load_to_sql(current_date, url, connection):
                sucessful_uploads += 1
    logging.info(" {} Successful Uploads".format(sucessful_uploads))

def test():
    test_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/04-04-2020.csv"
    test_date="04-04-2020"
    print(load_to_sql(test_date, test_url))

def lambda_handler(event, context):
    #logging info
    if ENV_TYPE == 'dev':
        logging.basicConfig(level=logging.DEBUG)
        from dotenv import load_dotenv
        load_dotenv()
    elif ENV_TYPE == 'prod':
        logging.basicConfig(level=logging.INFO)

    logging.info("Date: {}".format(date.today().strftime("%m-%d-%Y")))
    engine_string = 'mysql://{}:******@{}:{}/{}'.format(os.getenv("SQL_USER"),os.getenv("SQL_HOST"), os.getenv("SQL_PORT"),os.getenv("SQL_DB"))
    logging.info("Engine: {}".format(engine_string))
    uploads = 0
    #Running
    if ENV_TYPE == 'prod':
        uploads = main(5)
    else:
        engine_string = 'mysql://{0}:{1}@{2}:{3}/{4}'.format(os.getenv("SQL_USER"),os.getenv("SQL_PASSWORD"),os.getenv("SQL_HOST"), os.getenv("SQL_PORT"),os.getenv("SQL_DB"))
        print("Engine:\n{}".format(engine_string))

        userin = input("\nContinue (y/n): ")
        if userin != 'y':
            exit(0)
        else:
            print("Specify Upload Contents\n\t1. Daily Covid Records\n\t2. US Zip Codes")
            userin = input("Input (1, ..., n): ")
            if(int(userin) == 1):
                print("Dates:\n\t1. All\n\t2. Past 5 Days\n\t3. Number of Past Days")    
                date_range = 5
                userin = int(input("Input (1, ..., n): "))
                if userin == 1:
                    uploads = main(-1)
                elif userin == 2:
                    uploads = main(5)
                elif userin == 3:
                    userin = int(input("Enter Number: "))
                    uploads = main(userin)
                else:
                    print("No matching Input")
                    exit(0)
            elif(int(userin) == 2):
                upload_zip_codes("uszipcodes.csv")
            else:
                print("No matching input")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successful'),
        'uploadCount': uploads
    }
lambda_handler(None, None)