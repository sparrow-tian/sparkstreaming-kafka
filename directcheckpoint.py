import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import *
#from kazoo.client import KazooClient


def printOffsetRanges(rdd):
     for o in rdd.offsetRanges():
         print('ok, lets start')
         print(type(o))
         print(o)
         print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)
         print('finished')
def createContext(checkpoint):
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 10)
    brokers, topic = sys.argv[1:]
    topics=[i for i in topic.split(',')]
    #print(brokers,type(brokers))
    #spicify consume offsite, can not spicify when use checkpoint, it's clashed with checkpoint
    #fromOffsets={TopicAndPartition(topic,i):0 for i in range(1)}
    #kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers},fromOffsets)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    ssc.checkpoint(checkpoint)
    lines.pprint()
    print(type(lines))
    print(type(kvs))
    kvs.foreachRDD(printOffsetRanges)
    kvs.foreachRdd(write_raw_func)
    #kvs.foreachRDD(save_offsets)
    #ssc.start()
    #ssc.awaitTermination(30)
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 3:
        #print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
	print('wrong parment')
        exit(-1)
    checkpoint='/hdfsfolder/checkpoint/'
    ssc=StreamingContext.getOrCreate(checkpoint,lambda: createContext(checkpoint))
    ssc.start()
    ssc.awaitTermination()
