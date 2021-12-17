import uuid
import boto3
import time
import json
from pprint import pprint

def write_record(json_record):
    partitionkey = str(uuid.uuid4())[:8]
    put_response = kinesis_client.put_record(StreamName=streamname,Data=json_record,PartitionKey=partitionkey)
def get_record(id):
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=streamname, ShardId=id, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    data_from_kinesis = kinesis_client.get_records(ShardIterator=shard_iterator)
    return data_from_kinesis
def list_shard():
    shards=kinesis_client.list_shards(StreamName=streamname)
    shard_list=[i["ShardId"] for i in shards["Shards"]]
    return shard_list
def list_number_of_records_per_shard():
    list_of_shards=list_shard()
    for i in list_of_shards:
        print("ShardID "+str(i)+" Length:"+str(len(get_record(i)["Records"])))
def list_records_per_shard(id):
    print([[i["Data"],i["ApproximateArrivalTimestamp"]] for i in get_record(id)["Records"]])

if __name__=="__main__":
    streamname = 'probe-poc'
    session = boto3.session.Session() 
    kinesis_client = session.client('kinesis', region_name='ap-south-1')
    test_record = json.dumps({
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
      })
    write_record(test_record)
    """list_of_shards=list_shard()
    for i in list_of_shards:
        list_records_per_shard(i)
        break"""
    
