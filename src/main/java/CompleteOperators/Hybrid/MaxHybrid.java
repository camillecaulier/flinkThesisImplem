package CompleteOperators.Hybrid;

import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.SwitchNodeEventBasic;
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindow;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunctionEvent;
import sourceGeneration.CSVSourceParallelized;

import java.time.Duration;

public class MaxHybrid {
    private String csvFilePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;

    public MaxHybrid(String csvFilePath, StreamExecutionEnvironment env) {
        this.csvFilePath = csvFilePath;
        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(500))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_ONCE, 1000).setParallelism(1)
                .flatMap(new CSVSourceParallelized()).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy);


        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = mainStream
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag)).setParallelism(1);

        //basic operator
        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunctionEvent());

        // time to do the thingy
        DataStream<EventBasic> operatorSplitStream = popularFilterStream.getSideOutput(operatorAggregateTag);

        //how to find the number of partitions before
        DataStream<EventBasic> split = operatorSplitStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(new MaxPartialFunctionFakeWindow(1000)).setParallelism(1);


//        here we can actually use the windows
//        DataStream<EventBasic> reconciliation = split
//                .partitionCustom(new SingleCast(), value->value.key )
//                .process(new MaxPartialFunctionFakeWindow(1000));//.setParallelism(1);

//        DataStream<EventBasic> reconciliation = split.keyBy(value-> value.key)
//                .process(new MaxPartialFunctionFakeWindowKeyed(1000));//.setParallelism(1);

        DataStream<EventBasic> reconciliation = split
                .keyBy(value-> value.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunctionEvent()).setParallelism(1);



//        reconciliation.print("reconciliation").setParallelism(1);


        return reconciliation.union(operatorBasicStream);


    }
}
