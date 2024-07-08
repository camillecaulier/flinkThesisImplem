package testingStuff;

import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.SwitchNodeEventBasic;
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindow;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunction;
import sourceGeneration.CSVSourceFlatMap;

import java.time.Duration;

public class testingStuffFakeWindow {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy
                .<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(500)) // Example delay: 100 ms
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
                .flatMap(new CSVSourceFlatMap()).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy);


        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = mainStream
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag,10)).setParallelism(1);


        //basic operator
        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunction());




        // time to do the thingy
        DataStream<EventBasic> operatorAggregateStream = popularFilterStream.getSideOutput(operatorAggregateTag);


        //how to find the number of partitions before
        DataStream<EventBasic> split = operatorAggregateStream
                .partitionCustom(new RoundRobin(1), value->value.key ) //any cast
                .process(new MaxPartialFunctionFakeWindow(1000)).setParallelism(1);


//        DataStream<EventBasic> reconciliation = split.keyBy(value-> value.key)
//                .process(new MaxPartialFunctionFakeWindowKeyed(1000));//.setParallelism(1);

        DataStream<EventBasic> reconciliation = split
                .keyBy(value-> value.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunction()).setParallelism(1);



        reconciliation.print("reconciliation").setParallelism(1);


        env.execute("Key Group Metric Example");
    }
}
