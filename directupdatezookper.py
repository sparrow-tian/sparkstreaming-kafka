import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *
from kazoo.client import KazooClient


def get_zookeeper_instance():
    if 'KazooSingletonInstance' not in globals():
        globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
        globals()['KazooSingletonInstance'].start()
    return globals()['KazooSingletonInstance']


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def read_offsets(zk, topics):
    from_offsets = {}
    #partitions = [int(i) for i in zk.get_children('/brokers/topics/{}/partitions'.format(topic))]
    for topic in topics:
        #for partition in zk.get_children('/consumers/{}'.format(topic)):
        for partition in zk.get_children('/brokers/topics/{}/partitions'.format(topic)):
            topic_partion = TopicAndPartition(topic, int(partition))
            try:
                offset = int(zk.get('/consumers/{}/{}'.format(topic,partition))[0])
            except:
                offset = 0
                print('/consumers/{}/{} is empty'.format(topic,partition))
            from_offsets[topic_partion] = offset
    print(from_offsets)
    return from_offsets

def save_offsets(rdd):
    for offset in rdd.offsetRanges():
        #print(offset)
        path = '/consumers/{ttopic}/{tpartition}'.format(ttopic=offset.topic,tpartition=offset.partition)
        zk.ensure_path(path)
        zk.set(path, str(offset.untilOffset).encode())

def check_new_partition(rdd):
    new_partition=[int(i) for i in zk.get_children('/brokers/topics/{}/partitions'.format(topic))]
    current_partition=rdd.offsetRanges()
    print(current_partition) 
    print(new_partition)
    if(len(new_partition)>len(current_partition)):
        print('Added some partitions,updating zookeeper....')
        save_offsets(rdd)
        print('Zookeeper updated successfully....')
        #sys.exit()
        ssc.stop()
def save_db(rdd):
    for o in rdd.offsetRanges(): 
def printOffsetRanges(rdd):
     print rdd.offsetRanges()
     for o in rdd.offsetRanges():
         print(type(o))
         print(o)
         print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)
def createContext(fromOffsets,sc):
    ssc = StreamingContext(sc, 20)
    print(type(fromOffsets))
    print(fromOffsets)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers},fromOffsets)
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    print(type(lines))
    print(type(kvs))
    kvs.foreachRDD(check_new_partition)
    kvs.foreachRDD(printOffsetRanges) 
    kvs.foreachRDD(save_offsets)
    return ssc

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        #print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
	print('wrong parment')
        exit(-1)
    ZOOKEEPER_SERVERS = "zookeeper_server:2181"
    zk = get_zookeeper_instance()
    brokers, topic , refresh, partition_offsets = sys.argv[1:]
    fromOffsets=read_offsets(zk,topics)
    if refresh == 'refresh':
        for partition_offset in partition_offsets.split(','):
            partition = int(partition_offset.split(':')[0])
            offset = int(partition_offset.split(':')[1])
            fromOffsets[TopicAndPartition(topic, partition)]= offset
        print(fromOffsets)
    while True:   
        sc = SparkContext(appName="PythonStreamingKafkaWordCount")
        ssc=createContext(fromOffsets,sc)
        ssc.start()
        ssc.awaitTermination()
        time.sleep(10)
        fromOffsets=read_offsets(zk,topics)

