import uuid
import boto3
import time
import json
from pprint import pprint
session = boto3.session.Session() 
kinesis_client = session.client('firehose', region_name='ap-south-1')

def send_test_payload_directly_to_firehose(payload):
	response=kinesis_client.put_record(
    	DeliveryStreamName='PUT-S3-ciobV',
    	Record={'Data': json.dumps(payload)})

if __name__=="__main__":
	payload={
      'difftime':9600,
      'version':'1',
      'sdkversion':'1',
      'player':'ExoPlayer',
      'playerapp':'TataSkyBinge',
      'cdn':'Akamai',
      'ip':19216801,
      'provider':'SonyLiv',
      'udid':'181F0C08-23BD-4590-ABHJSRLHP0U8',
      'platform':'iOS',
      'devicetype':'Mobile',
      'manufacturer':'Samsung',
      'model':'Galaxy 5',
      'networktype':'Cellular-4G',
      'sessionid':'OPERNHUI-78FG-LOPI-KILNGTHVKODT',
      'time_stamp':1639431064,
      'playbackposinsec':1029,
      'videoid':'103KiOP',
      'assetduration':60000,
      'framerate':25,
      'acodec':'ac3',
      'vcodec':'hevc',
      'bitrate':2000000,
      'resolution_height':1920,
      'reolution_width':1080,
      'throughput':2200000,
      'durationofplayback':30,
      'stall_count':2,
      'stall_duration':2400,
      'has':'HLS',
      'drm':'PlayReady',
      'live':'true',
      'event':'STARTED',
      'eventdata_latency':2400
      }
	send_test_payload_directly_to_firehose(payload)