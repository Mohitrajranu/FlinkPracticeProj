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
										 /                   \
										/                     \
									  /                        \
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

Iterative Stream accepts feedback to itself and comprises of FeedbackStream and OutputStream. If condition is met then go to output stream
Data is of the following schema

# cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count

Using Datastream/Dataset transformations find the following for each ongoing trip.

1.) Popular destination.  | Where more number of people reach.

2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.

3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made

Questions for this assignment
What all transformation operations you will use?
map, filter, reduce, groupby, sum, maxby

Windowing : Windows split the data stream into buckets of finite size over which computation can be applied.
Tumbling Windows : Time based, the next window starts after first window ends.
Sliding Windows : Time based , windows overlap.
Window will emit the result once the specific time limit is passed
Non Keyed Stream uses WindowAll() , whereas Keyed Stream use Window() assigner, Window assigner defines how entities are assigned to windows.
Session Windows : Created based on activity , Doesnot have fix start or end time.Window closes whenever there is a gap of inactivity , 
A new window is created for every event , and if two or more windows lie in the same time range then the window is said to be mergeable and merged into a single window.
Global Windows : 1 window per key , do computations with the help of trigger

Processing Time :: System time of machine which executes task.
Source-->Flink Ingestion-->Processing
If TimeCharacteristic.ProcessingTime is used then notion is processing time.
Window will system clock of the machine
Simplest notion of time requires no co-ordination between streams and machines
best performance and low latency ., less suitable for distributed environment.

Event Time :: Event time is time at which event occured on source
Source-->Flink Ingestion-->Processing
Event time is embedded within each record
consistent and deterministic results regardless of order they arrive at flink.
shows latency while waiting out of order events.

Ingestion Time :: Each record gets source's current timestamp.
Source-->Flink Ingestion-->Processing
All time based operations refer to that timestamp.
Ingestion time uses stable timestamp
can-not handle out-of-order events or late data.

TRIGGERS :: Trigger determines when a window is ready to be processed, All window assigners comes with default triggers.
public abstract TriggerResult onElement (T element,long timestamp,W window,TriggerContext ctx)
public abstract TriggerResult onEventTime (long time,W window,TriggerContext ctx)
public abstract TriggerResult onProcessingTime (long time,W window,TriggerContext ctx)
public void onMerge(W window,OnMergeContext ctx)
public abstract void clear(W window,TriggerContext ctx)

Return-Types for TriggerResult are of 4 types ::
CONTINUE : do nothing [on element]
FIRE : Trigger the computation[default]
PURGE : Clear contents of window
FIRE_AND_PURGE : Trigger the computation and clear contents of window after it.

EventTime Trigger :: This trigger fires based upon progress of event time.
                     .trigger(EventTimeTrigger.create())
ProcessingTime Trigger :: This Trigger fires based upon progress of processing time.
                      .trigger(ProcessingTimeTrigger.create())
Count Trigger :: This Trigger fires when the number of elements in a window exceeds the count specified in parameters.
                      .trigger(CountTrigger.of(5))       
Purging Trigger :: This trigger takes another trigger as argument and purger it after the inner one fires.
                      .trigger(PurgingTrigger.of(CountTrigger.of(5)))       
                      
EVICTORS :: Evictors is used to remove elements from a window after the trigger fires and before and/or after the window function is applied.

Window Created         -> Trigger         ->     Window Function           -> Result
.window(),.windowall()     .trigger()      |->     reduce,fold,aggregate etc  ^                                            
                                           |___ Evictor_______________________|
                                           .evictor()
void evictBefore
void evictAfter

CountEvictor :: keeps the user-specified number of elements from the window and discard the remaining ones, 
                .evictor(CountEvictor.of(4))       

DeltaEvictor :: takes a deltafunction and a threshold as arguments, computes a delta between the last element in the window
 and the remaining elements and then removes those elements whose delta is greater or equal to threshold.
                .evictor(DeltaEvictor.of(threshold,new MyDelta()))   
TimeEvictor :: takes argument as an interval in milliseconds and for a given window it finds the maximum timestamp
max_ts amongst its elements and removes all those elements with timestamps smaller than max_ts-interval
                .evictor(TimeEvictor.of(Time.of(evictionsec,TimeUnit.SECONDS)))       
                
---> Mechanism to measure progress of event time in Flink is called watermarks. A watermark declares the amount of event time 
passed in the stream.
--> Late elements are the elements that arrive after the watermark has crossed the elements timestamp value.
Allowed lateness is the time by which a element can be late before it is dropped , elements with timestamp = (Watermark + Allowed Lateness)
are still added in the window , default value of Allowed Lateness is 0.Late elements may cause the window to fire again with updated
results. Flink keeps a state of Window until the allowed lateness time expires. The output will contain multiple results for the
same computation.
SIDE Output :
Demo<T> result = input.keyBy().window().allowedlatenes(time).sideOutputLateData(lateOutputTag).
                 <windowed transformation>(<window function>)   -> ProcessFunction,CoProcessFunction,ProcessWindowFunction,ProcessWindowAllFunction  
DataStream<T> lateStream = result.getSideOutput(lateOutputTag);

Built-in Watermark Generators::
.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyEvent>(Time.seconds(10))
{
public long extractTimestamp(MyEvent element){
return timestamp;
}
}                 
 
 .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyEvent>()
 {
 <logic>
 }
 )     
 
 State :: State is a snapshot of a application(operators) at any particular time which remembers information about past input/events
  which will affect the future output.
  state can be used in any of the following ways:
  To search for certain event patterns happened so far.
  To train a Machine learning model over a stream of data points , To manage historic data it allows efficient access to past events
  To achieve fault-tolerance through checkpointing. A state helps in restarting the system from the failure point.
  To rescale jobs(parallelism). During the rescaling operation flink takes the previous saved states from the persisted disk such as hdfs.
  and distribute it across the new jobs.
  To convert stateless transformations to stateful transformations.
  
  Stateless Transformation :: Current output is dependent on current input element,independent of previous input elements.
  No need to accumulate any data.
  ex :-> Map,FlatMap,Filter
  Stateful Transformation :: Current output is dependent on current input element and previous inputs.Need to accumulate previous input.
  Ex :-> Reduce,Sum,Aggregate
  State Objects:: ValueState<T>,ReducingState<T>,ListState<T>
  
Fault Tolerance and checkpointing :: Fault Tolerance in flink ensures that in case of failures the application will be recovered fully
 and the application will be restarted exactly from the failure point.
 
Checkpointing :: Checkpointing is to consistently draw snapshots of distributed data stream and corresponding operator state.
Each drawn snapshot will hold a full application state till the checkpointed time.
Snapshots/Checkpoints are light weight and does not impact much on performance.
After the snapshot is taken it is saved into a persistent storage(State backend) ex:-> HDFS
crash -> stop the dataflow -> take the state from the latest checkpoint -> According to the latest checkpoined state
=>Reset Data Stream => Restart Operator/Application
DataStream source/message queue broker should have the ability to rewind the data stream example:: Apache Kafka

Barrier Snapshotting: Flink will take a snapshot/create a checkpoint based on stream barriers.
Synchronous Snapshot:: Flink operators will stop processing new records while snapshot/checkpoint is being written.
Asynchronous Snapshot:: Flink operators will not stop processing new records while snapshot/checkpoint is being written.
-> Use a managed state , Use a state backend that supports asynchronous snapshotting.

State Backend :: State backend determines how and where a checkpoint state is stored.
Memory StateBackend :: Stores the state's data internally as objects on Java Heap.
FS StateBackend :: Stores the state's data internally into specified filesystem ex-HDFS
RocksDb StateBackend :: Stores the in-flight data (temp data) in RocksDb database, upon checkpointing the same database is 
written to Filesystem ex:-HDFS

Incremental Checkpointing :: In Incremental checkpointing, we do not write the full state for every checkpoint rather for consecutive
states we will only save the delta(changes between 2 checkpoints).
Disadvantages of normal checkpointing:
->In case of synchronous snapshotting, writing a huge state will increase the job processing time.
->Writing checkpoints with Gb's ,Tb's of state very frequently to disk consumes disk space and degrades overall performance.
To enable incremental checkpointing :-> RocksDBStateBackend backend = new RocksDBStateBackend(filebackend,true);
set the flag to true.

Types of States :-> 
Operator State: State is bound to 1 operator instance,1 state per operator instance.
Keyed State: 1 State per operator instance per key[operator state partitioned as key]
Operator and keyed state are further categorised into Managed state and Raw state

Managed State :: Totally controlled by Flink saved in HashTables. ValueState<T>,ReducingState<T>,ListState<T>
Raw State :: Controlled by operators itself, in their own data structures.

Managed Keyed state::  ValueState<T>,ReducingState<T>,ListState<T>,AggregatingSate<T>

ValueState<T> :Maintains a single value in it
ListState<T> :Maintains a list of state.
ReducingState<T> : Reducing state keeps a single value in it and that value is sum of all the elements added in the state so far.

Managed Operator state:
It is available to all the operations.
public class StateDemo implements SinkFunction<> ,Checkpointedfunction/ListCheckpointedFunction
Checkpointedfunction interface methods:
void snapshotState(FunctionSnapshotContext context) throws Exception:
void initializeState(FunctionInitializationContext context) throws Exception:
SinkFunction Interface Methods:
public void invoke() throws Exception

Restart Strategies:: 
Fixed Delay Restart Strategy: Flink attempts a given no of time t, restart the job before it fails.
 env.setRestartStrategy(RestartStrategies.fixedDelayRestart(max no of restart attempts, delay Time));
Failure Rate Restart Strategy:Flink will keep on trying restarting till failure rate exceeds.
env.setRestartStrategy(RestartStrategies.failureRateRestart(failure rate, time interval for measuring failure rate,delay));
No Restart Strategy : Flink will not try to restart the job.
env.setRestartStrategy(RestartStrategies.noRestart());
Fallback Restart Strategy : The cluster defined restart strategy is used.

BROADCAST STATE::
Broadcast state is used in cases where some data is required to be broadcasted to all the running nodes and tasks in cluster.
The broadcasted stream/state is saved locally on each machine and all tasks running on a machine will access it locally.

For Key --> KeyedBroadcastProcessFunction
For non Key --> BroadcastProcessFunction

QUERYABLE STATE::
Queryable State is used to expose Flink's managed keyed state to the outside world and allows users to query that state from outside flink.
Example: A simple java program will access a flink's program managed state(if set to Queryable state)

Architecture Components:
QueryableStateClient : Runs outside Flink cluster and submits the user queries.
QueryableStateClientProxy : Runs inside the flink cluster and is responsible for receiving the client's queries.
QueryableStateServer : Runs inside the Flink cluster on each TaskManager and is responsible for serving the locally stored state.

  2                                       3
1Client :Request for state of k1 key-->2Proxy:Query for state of k1 key from T1-->3State Server
                                         ||
2.1.Fetch taskmanager for k1 key(From the proxy)  < Job Manager> 2.2.Return TaskManager(T1) for k1 key(to the proxy)       

Case Study :
Twitter data analysis: create a twitter developer account , using twitter credentials apps.twitter.com

TABLE and SQL APIs
core idea of both api is same i.e., to process the data in table form.Same syntax for creating and registering tables.
both are tightly integrated with other apis like DataStream and DataSet.Can switch between apis within a program
Table api is a language integrated(java or scala) query api.No Sql like syntax , SQL API is based on Apache Calcite which implements SQL standard.SQL like syntax.

Gelly API for GRAPH PROCESSING
A graph is an ordered set of vertices and edges, vertices are entities and edges are relationship between entities.



                     