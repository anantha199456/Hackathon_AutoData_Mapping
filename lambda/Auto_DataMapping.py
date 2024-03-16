import json
import boto3
import pandas as pd
#from gensim.models import Word2Vec
from difflib import get_close_matches
from fuzzywuzzy import process
import csv
import pymysql
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
s3_client = boto3.client('s3')

'''
This method is used to detect the type of delimeter used in CSV/TXT file.
'''
def detect_delimiter(bucket_name, file_key, sample_size=1024):
    s3 = boto3.client('s3')
    
    # Download a sample of the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = response['Body']
    sample_data = body.read(sample_size)
    
    # Guess the delimiter
    dialect = csv.Sniffer().sniff(sample_data.decode('utf-8'))
    return dialect.delimiter

'''
This method is used to read the source files and depending on the extension(csv,xls,json) we convert 
the data to Pandas Dataframe for futher processing
'''
def read_file_basedExtension(file_path,bucket_name,file_name):
    # Get the file extension
    file_extension = file_path.split('.')[-1].lower()
    print(file_extension)
    # Read the file based on the file extension
    if file_extension == 'csv' or file_extension == 'txt':
        # Detect the delimiter for CSV/TXT files
        delimiter = detect_delimiter(bucket_name, file_name)
        print("Detected delimiter:", delimiter)

        df = pd.read_csv(file_path, delimiter=delimiter)
        df.head()
    elif file_extension == 'xls' or file_extension == 'xlsx':
        df = pd.read_excel(file_path)
    elif file_extension == 'json':
        df = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    return df

'''
This method will use the Python's fuzzywuzzy library to match columns based on the Target Columns we provide
and returns function to control the similarity score threshold for considering a match.
'''
def match_columns_with_fuzzywuzzy(source_columns, target_columns, threshold=70):
    matched_columns = []
    non_matched_columns = []

    # Iterate through target columns and find the closest match from the source
    for target_column in target_columns:
        best_match, score = process.extractOne(target_column, source_columns)
        if score >= threshold:
            matched_columns.append((target_column, best_match, score))
        else:
            non_matched_columns.append((target_column, best_match, score))
    return matched_columns,non_matched_columns

'''
This method is used display the matched columns results of Fuzzywuzzy library in Console Log.
'''   
def display_matched_columns(matched_columns):
    print("Matched Columns:")
    for target_column, best_match, score in matched_columns:
        print(f"Target column: {target_column} - Best match: {best_match} - Score: {score}")

'''
This method connects to MySQL RDS Table and appends the Columns based of the score that the fuzzywuzzy model provide. 
This is used as Report Table.
Table Name : hackathon_debug_report
''' 
def append_matched_columns_to_rds_report_tbl(matched_columns, rds_config, table_name):
    #connect to a MySQL database and perform operations such as executing queries, fetching data, and updating records
    connection = pymysql.connect(**rds_config)

    # Iterate over matched columns and append to RDS table
    cursor = connection.cursor()

    for _, row in matched_columns.iterrows():
        # Assuming 'df' has columns: 'Target column', 'Best match', 'Score', 'Status'
        query = f"INSERT INTO {table_name} (target_column, best_match, score, status) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (row['Target column'], row['Best match'], row['Score'], row['Status']))
    connection.commit()
    cursor.close()
    connection.close()

'''
This method connects to MySQL RDS Table and based on the table name provided, it Truncates the complete table.
'''
def truncate_tables(rds_config,report_table):
    #connect to a MySQL database and perform operations such as executing queries, fetching data, and updating records
    connection = pymysql.connect(**rds_config)

    # Iterate over matched columns and append to RDS table
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {report_table};")
    connection.commit()
    cursor.close()
    connection.close()

'''
This method connects to MySQL RDS Table and appends the source data incoming from the S3 location
Table Name : hackathon_debug_employee
''' 
def append_souce_data_to_rds(source_data, rds_config, table_name):
    connection = pymysql.connect(**rds_config)
    print('source_data contents')
    print(source_data)
    # Append data to RDS table
    cursor = connection.cursor()
    for _, row in source_data.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        query = f"INSERT INTO {table_name} ({', '.join(row.index)}) VALUES ({placeholders})"
        cursor.execute(query, tuple(row.values))
    
    connection.commit()
    cursor.close()
    connection.close()

'''
This method use AWS SES Service to send email report which consist of the 
Target Column, Best Column Matched, Score(Threshold Match %) , Mapped Columns to the provided recipient_email address.
'''
def send_email_with_dataframes(df1, df2, recipient_email, sender_email,file_name):
    # Convert DataFrames to HTML tables
    html_content1 = df1.to_html(index=False)
    html_content2 = df2.to_html(index=False)

    # Create a multipart message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Data Mapping Analysis Report'
    msg['From'] = sender_email
    msg['To'] = recipient_email

    # Create HTML content
    html_body = f"""
    <html>
    <head></head>
    <body>
    <p>Here are the results of Data Mapping Analysis:</p>
    <p><b>Unique File Name:  {file_name}</b></p>
    <p><b>Matched Columns:</b></p>
    {html_content1}
    <p><b>Non Matched:</b></p>
    {html_content2}
    <br>
    <br>
    <br>
    <p><b> Regards </b></p>
    <p><b> Debug Kings </b></p>
    </body>
    </html>
    """

    # Attach HTML content to the email
    msg.attach(MIMEText(html_body, 'html'))

    # Send the email
    ses_client = boto3.client('ses', region_name='us-east-1')
    try:
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Error sending email:", e)


'''
The main menthod for Lambda Invocation.
'''
def lambda_handler(event, context):
    
    # Read the filename from the event
    file_name = event['file_name']
    print("The Source file name that is received :" + file_name)

    # Read the file from S3 bucket
    bucket_name = os.environ['bucket_name']
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_data = response['Body'].read().decode('utf-8')
    
    file_path = "s3://"+bucket_name+"/"+file_name
    print(file_path)

    # Process the file to generate the report
    # For demonstration, let's assume the report is just the file content
    # calculate file based on extension.
    dataframe = read_file_basedExtension(file_path,bucket_name,file_name)
    df=dataframe
    print(df.head())
  
    # Target columns that needs to be mapped with the Source File Columns.
    str_target_columns = os.environ['target_columns']
    target_columns = list(str_target_columns)

    # Extract source columns from the DataFrame
    source_columns = df.columns.tolist()
    print(source_columns)

    # Match columns using fuzzywuzzy python package
    matched_columns,non_matched_columns = match_columns_with_fuzzywuzzy(source_columns, target_columns)

    # Display Matched columns along with there Taget Column details.
    display_matched_columns(matched_columns)
    
    # Converting the List to Pandatas Dataframe for RDS table insertion
    # Matched Records
    df_matched = pd.DataFrame(matched_columns, columns=["Target column", "Best match", "Score"])
    df_matched['Status'] = 'Matched'

    # Non-Matched Records
    df_non_matched = pd.DataFrame(non_matched_columns, columns=["Target column", "Best match", "Score"])
    df_non_matched['Status'] = 'Not Matched'

    recipient_email = os.environ['recipient_email']
    sender_email = os.environ['recipient_email']
    # Sending email using AWS SES service
    send_email_with_dataframes(df_matched,df_non_matched,recipient_email,sender_email,file_name)
    
    # Merge both the dataframes to provide output to the RDS table
    # Concatenate matched_df and non_matched_df
    df_merged_data = pd.concat([df_matched, df_non_matched], ignore_index=True)

    # Append matched columns to RDS table
    host = os.environ['host']
    user = os.environ['user']
    password = os.environ['password']
    port = os.environ['port']
    db = os.environ['db']

    rds_config = {
    "host": host,
    "user": user,
    "password": password,
    "port": 3306,
    "db": db
    }
    
    #Truncate Report table
    hackathon_report_table = os.environ['hackathon_report_table']
    truncate_tables(rds_config,hackathon_report_table)
    
    print(df_merged_data.columns)
    # Report RDS Table data appending
    append_matched_columns_to_rds_report_tbl(df_merged_data, rds_config, hackathon_report_table)

    # Read the source file
    # Extract data corresponding to matched columns
    matched_column_names = list(df_matched["Best match"])
    mapped_data = df[matched_column_names]
    # Raname the Source Column names with the Static Columns used/provided.
    mapped_data.rename(columns=dict(zip(mapped_data.columns, target_columns)), inplace=True)
    
    print('After Renaming Mapped data Results')
    print(mapped_data)
    
    #Handling columns which are not matched to NULL/NONE
    missing_columns = set(target_columns) - set(mapped_data.columns)
    for col in missing_columns:
        mapped_data[col] = None
    
    mapped_data = mapped_data[target_columns]
    mapped_data['source_file_name'] = file_name
    
    # Source Data RDS Table data appending
    hackathon_source_table = os.environ['hackathon_source_table']
    append_souce_data_to_rds(mapped_data, rds_config, hackathon_source_table)

    # Return the report
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully Completed')
   }