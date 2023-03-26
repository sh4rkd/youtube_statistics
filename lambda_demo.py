import json
import base64
import pandas as pd
import time
import requests
import random
import time
import boto3
from datetime import datetime
import datetime
import os
import urllib
from urllib import request, parse

s3_client = boto3.client('s3')

def get_stats(api_key,channel_id):
    
    url_channel_stats = f'https://youtube.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={api_key}'
    response_channels = requests.get(url_channel_stats)
    channel_stats = json.loads(response_channels.content)
    channel_stats = channel_stats['items'][0]['statistics']
    date = pd.to_datetime('today').strftime("%Y-%m-%d")

    data_channel = {

            'Date':date,
            'Total_Views':int(float(channel_stats['viewCount'])),
            'Subscribers':int(float(channel_stats['subscriberCount'])),
            'Video_count':int(float(channel_stats['videoCount']))
                        }

    return data_channel

def channels_stats(df,api_key):
    
    date = []
    views = []
    suscriber = []
    video_count = []
    channel_name = []
    
    tiempo = [1,2.5,2]
    
    for i in range(len(df)):
        
        stats_temp = get_stats(api_key,df['Channel_id'][i])
        
        channel_name.append(df['Channel_name'][i])
        date.append(stats_temp['Date'])
        views.append(stats_temp['Total_Views'])
        suscriber.append(stats_temp['Subscribers'])
        video_count.append(stats_temp['Video_count'])
     
    time.sleep(random.choice(tiempo))
    
    data = {
        
        'Channel_name':channel_name,
        'Subscribers':suscriber,
        'Video_count':video_count,
        'Total_Views':views,
        'Createt_at':date,
    }
    
    df_channels = pd.DataFrame(data)
    
    return df_channels




def lambda_handler(event, context):
    

    bucket_name = os.environ['BUCKET_INPUT']
    filename =  os.environ['FILE_CHANNELS']
    DEVELOPER_KEY = os.environ['API_KEY']
    bucket_output = os.environ['BUCKET_OUTPUT']
    TWILIO_ACCOUNT_SID = os.environ['TWILIO_ACCOUNT_SID']
    TWILIO_AUTH_TOKEN = os.environ['TWILIO_AUTH_TOKEN']
    twilio_phone_number = os.environ['TWILIO_PHONE_NUMBER']
    user_phone_number = os.environ['USER_PHONE_NUMBER']
    TWILIO_SMS_URL = "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json"
    TWILIO_CALL_URL = "https://api.twilio.com/2010-04-01/Accounts/{}/Calls.json"
    # Get File from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key= filename)
    df_channels = pd.read_csv(obj['Body']) # 'Body' is a key word
    
    
    results = channels_stats(df_channels,DEVELOPER_KEY)
    ytb_date = pd.to_datetime('today').strftime("%Y%m%d")
    
    results.to_csv(f'/tmp/youtube_stats_{ytb_date}.csv',index = False)
 
    # Send file to  S3
    s3 = boto3.resource("s3")
    
    s3.Bucket(bucket_output).upload_file(f'/tmp/youtube_stats_{ytb_date}.csv', Key=f'youtube_stats_{ytb_date}.csv')
    os.remove(f'/tmp/youtube_stats_{ytb_date}.csv')
    
    to_number = user_phone_number
    from_number = twilio_phone_number
    body = f'Buenas papirruquis el archivo 📁\ndel dia {date.today()}📅\n🎉Ya se almaceno en tu Bucket🎉\nLo encontraras como -> youtube_stats_{ytb_date}.csv 😎'
    action = "WHATSAPP"

    if not TWILIO_ACCOUNT_SID:
        return "Unable to access Twilio Account SID."
    elif not TWILIO_AUTH_TOKEN:
        return "Unable to access Twilio Auth Token."
    elif not to_number:
        return "The function needs a 'To' number in the format +12023351493"
    elif not from_number:
        return "The function needs a 'From' number in the format +19732644156"
    elif not body:
        return "The function needs a 'Body' message to send."
    elif not action:
        return "The function needs a 'Action' SMS/WhatsApp/Call."

    if action == "WHATSAPP":
        from_number = "whatsapp:" + from_number
        to_number   = "whatsapp:" + to_number

    # insert Twilio Account SID into the REST API URL
    if action == "CALL":
        populated_url = TWILIO_CALL_URL.format(TWILIO_ACCOUNT_SID)
        post_params = {"To": to_number, "From": from_number, "Url": body}
    else:
        populated_url = TWILIO_SMS_URL.format(TWILIO_ACCOUNT_SID)
        post_params = {"To": to_number, "From": from_number, "Body": body}
    

    

    # encode the parameters for Python's urllib
    data = parse.urlencode(post_params).encode()
    req = request.Request(populated_url)

    # add authentication header to request based on Account SID + Auth Token
    authentication = "{}:{}".format(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    base64string = base64.b64encode(authentication.encode('utf-8'))
    req.add_header("Authorization", "Basic %s" % base64string.decode('ascii'))

    try:
        # perform HTTP POST request
        with request.urlopen(req, data) as f:
            print("Twilio returned {}".format(str(f.read().decode('utf-8'))))
    except Exception as e:
        # something went wrong!
        return e
    return action + " successfully!"
