package testingStuff;

import eventTypes.EventBasic;
import eventTypes.Value;
import keygrouping.RoundRobin;
import keygrouping.SingleCast;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.SwitchNodeEventBasic;
import popularKeySwitch.splitProcessFunction;
import processFunctions.MaxPartialFunctionFakeWindow;
import processFunctions.MaxPartialFunctionFakeWindowKeyed;
import processFunctions.MaxWindowProcessFunction;
import processFunctions.MaxWindowProcessFunctionEvent;
import sourceGeneration.CSVSource;
import sourceGeneration.CSVSourceParallelized;
import sourceGeneration.RandomStringSource;

import java.time.Duration;

public class testingStuffFakeWindow {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy
                .<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(1000)) // Example delay: 100 ms
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBasic>() {
                    @Override
                    public long extractTimestamp(EventBasic element, long recordTimestamp) {
                        return element.value.timeStamp;
                    }
                });

        //KEEP PARALLELISM TO 1 see testingStuffNewSourceReading for an attmept for multiparrallel

        String csvFilePath = "zipf_distribution100_5.csv";
//        DataStream<EventBasic> mainStream = env
//                .addSource(new CSVSource(csvFilePath));

        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_ONCE, 1000).setParallelism(1)
                .flatMap(new CSVSourceParallelized()).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy);


        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = mainStream
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag)).setParallelism(1);


        //basic operator
        DataStream<Tuple2<String,Integer>> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .process(new ProcessFunction<EventBasic, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(EventBasic value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.key, value.value.valueInt * 10));
                    }
                });



        // time to do the thingy
        DataStream<EventBasic> operatorAggregateStream = popularFilterStream.getSideOutput(operatorAggregateTag);


        //how to find the number of partitions before
        DataStream<EventBasic> split = operatorAggregateStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(new MaxPartialFunctionFakeWindow(1000)).setParallelism(5);


//        here we can actually use the windows
//        DataStream<EventBasic> reconciliation = split
//                .partitionCustom(new SingleCast(), value->value.key )
//                .process(new MaxPartialFunctionFakeWindow(1000));//.setParallelism(1);

        DataStream<EventBasic> reconciliation = split.keyBy(value-> value.key)
                .process(new MaxPartialFunctionFakeWindowKeyed(1000));//.setParallelism(1);

//        DataStream<EventBasic> aggregate = split
//                .keyBy(value-> value.key)
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .process(new MaxWindowProcessFunctionEvent()).setParallelism(1);

//        mainStream.print("mainStream").setParallelism(1);
//        operatorAggregateStream.print("operatorAggregateStream");
////        operatorBasicStream.print("operatorBasicStream");
////        popularFilterStream.print("popularFilterStream");
//        split.print("split").setParallelism(1);
        reconciliation.print("reconciliation").setParallelism(1);

        env.execute("Key Group Metric Example");
    }
}
