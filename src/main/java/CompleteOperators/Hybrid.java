//package CompleteOperators;
//
//import keygrouping.CustomStreamPartitioner;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//public class Hybrid {
//
//
//    public DataStream splitAggregate(CustomStreamPartitioner partitioner, DataStream input, ProcessFunction partialFunction, ProcessFunction aggregateFunction, int parallelism, int windowSize){
//        DataStream partialStream = input.process(partialFunction).setParallelism(parallelism);
//        DataStream aggregatedStream = partialStream.keyBy("key").window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize))).process(aggregateFunction).setParallelism(parallelism);
//        return aggregatedStream;
//    }
//
//    public DataStream
//
//}
