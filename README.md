# FlinkPracticeProj
Apache Flink :: open source stream processing framework for distributed,high performing , always available data streaming applications
Flink is a true processing framework, flinks stream is based on windowing and checkpointing, flink is implemented in java and has its own automatic memory manager , rarely gets out of memory. Flink uses controlled cyclic dependency graph as its execution engine.
It also provides support for batch processing , grapgh processing,iterative processing.
Flink gives low latency and high throughput applications. Robust,fault-tolernce, application can restart from exactly same point where it failed.
Flink has rich set of libraries for graph processing,Machine learning,String handling,relational apis etc.
Flink's application state is rescalable,possible to add resources while app is running.also maintains exactly-once semantics.

Zeeplin[Interactive Data Analysis]

Other Libraries[Relational--> TABLE ,Graph Processing--> GELLY,Machine Learning-->FLINK ML]

Abstraction[Dataset{Batch Processing} DataStream{Stream processing}]

Engine(Flink Runtime)

Deploy[Local JVM ] [Cluster {Standalone/Yarn}]	 [Cloud{GCE/EC2}]

Storage [Files{local,hdfs,s3}] [Databases{MongoDb,Hbase}] [Streams{Kafka,Flume,RabbitMq}]

Datsets are Immutable , does not support individual operations , Stores list of it dependencies like ds2 is dependent on ds1.

●	http://www.github.com/Mohitrajranu 
●	https://www.linkedin.com/in/mohitraj1

● readTextFile(path) -> Reads file linewise and returns them as set of strings.
● readCsvFile(path) -> Takes a csv file as input and returns a dataset of tuples.
● readFileofPrimitive(path,Class) -> Reads each line of file in form of a class mentioned in arguments(If you want to read the file as Integer class)
● readFileofPrimitives(path,delimeter,Class) -> Reads each line of file in the form of class mentioned in arguments using a delimeter.
● readHadoopFile(FileInputFormat,Key,Value,path) -> Reads hdfs file
● readSequenceFile(Key,Value,path) -> Reads sequence file.
cd flink-1.5.0
Starting flink cluster -> ./bin/start-cluster.sh
Running a filnk program , create an executable jar file -> ./bin/flink run /a/b/flinkprog.jar --input filename --output filename

JOIN Hint: 
OPTIMIZER_CHOOSES -> Not giving a hint at all leave it on the system
Flink will first broadcast the dataset to all the nodes then do join.
BROADCAST_HASH_FIRST-> if our first dataset is small , then we can broadcast it to each nodes internal memory so that shuffling is not needed for accesing the data.
BROADCAST_HASH_SECOND-> if our second dataset is small , then we can broadcast it to each nodes internal memory so that shuffling is not needed for accesing the data.
REPARTITION_HASH_FIRST-> when both inputs are large and the first is smaller than second one : Flink will partition each input if not parttioned and then build the hashtable of the first input set
REPARTITION_HASH_SECOND-> when both inputs are large and the second is smaller than first one : Flink will partition each input if not parttioned and then build the hashtable of the second input set
REPARTITION_SORT_MERGE-> Flink will partition each input if not parttioned and then sort each input and then perform join on the sorted input , do this only if one or the other input set is already sorted as it can have adverse effect on the performance.

Data Sources for DataStream Api

StreamExecutionEnvironment -> StreamExecutionEnvironment(object)<method>
● readTextFile(path) -> Reads file linewise and returns them as set of strings.
● readFile(FileInputFormat,path) -> Reads lines in the format as mentioned in the parameters.
● readFile(FileInputFormat,path,watchType,interval,pathFilter) -> Reads the file based on the provided fileinputformat watchType and scans the file periodically for any new data in every (x) ms where x is equal to interval value in milliseconds.
watchType-> FileProcessingMode.PROCESS_CONTINUOUSLY source is monitored periodically(based on the interval provided) for any new data 
FileProcessingMode.PROCESS_ONCE -> scan the path once read the data , create checkpoint and exits it will never visit that checkpoint again , total file is read in checkpoints : Slow recovery in case of node failure
                                         | Reading Process |
										 /                 \
										/                   \
									   /                     \
									Monitoring            Actual Reading
				●Scan path based on watchtype             ● Performed by multiple readers
				●Divide into splits                       ● Readers run parallely
				● Assign splits to readers                ● Each split read by only one readers
● socketTextStream -> Reads data from a socket,elements can be seperated by a delimeter
● addSource -> To add a custom data Source outside of Flink example: Kafka,Flume,Twitter API etc.

Data Sinks
● writeAsText()/TextOutputFormat -> Writes Output line wise , each line as a string.
● writeAsCsv(path,lines delimeter,fields delimeter)/CsvOutputFormat -> writes output as comma seperated values. Row and field delimeters configurable
● print() -> Prints the output to console , output is written as strings by internally calling toString() methods.
● writeUsingOutputFormat()/FileOutputFormat -> writes output as per the provided fileoutputformat.
● writeToSocket -> Writes elements to a socket according to a serialization schema
● addSink -> to add a custom data sink outside of Flink ex Kafka , Flume etc using connectors
