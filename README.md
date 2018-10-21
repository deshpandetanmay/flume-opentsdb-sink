# Flume Kafka to Open TSDB

--------------------------------------------------------------------
To use this sink, download the repo and build code using follwing commmand

```
mvn clean install
```

Copy generated JAR and place it in FLUME_HOME/bin

Sample Conf file

```
# Flume config to listen to Kakfa topic and write to OpenTSDB.
flume1.sources = kafka-source-1
flume1.channels = opentsdb-channel-1
flume1.sinks = opentsdb-sink-1
# For each source, channel, and sink, set
# standard properties.
flume1.sources.kafka-source-1.type = org.apache.flume.source.kafka.KafkaSource
flume1.sources.kafka-source-1.zookeeperConnect = localhost:2181
flume1.sources.kafka-source-1.topic = sensor
flume1.sources.kafka-source-1.batchSize = 100
flume1.sources.kafka-source-1.channels = opentsdb-channel-1
flume1.channels.opentsdb-channel-1.type = memory
flume1.sinks.opentsdb-sink-1.channel = opentsdb-channel-1
flume1.sinks.opentsdb-sink-1.type = in.co.hadooptutorials.flume.opentsdb.sink.HttpSink
flume1.sinks.opentsdb-sink-1.protocol  = http
flume1.sinks.opentsdb-sink-1.host  = <IP>
flume1.sinks.opentsdb-sink-1.port  = 4242
flume1.sinks.opentsdb-sink-1.path  = /api/put
flume1.sinks.opentsdb-sink-1.contentTypeHeader  = application/json
flume1.sinks.opentsdb-sink-1.acceptHeader  = application/json
# Other properties are specific to each type of
# source, channel, or sink. In this case, we
# specify the capacity of the memory channel.
flume1.channels.opentsdb-channel-1.capacity = 10000
```

To execute the Flume Agent, use follwing command

``` 
bin/flume-ng agent -n flume1 -c /usr/hdp/current/flume-server/conf -f /root/flume/opentsdb.conf -Dflume.root.logger=INFO,console
```
