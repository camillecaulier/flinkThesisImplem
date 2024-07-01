package CompleteOperators.Hybrid;

import CompleteOperators.CompleteOperator;
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
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunction;
import sourceGeneration.CSVSourceFlatMap;

import java.time.Duration;

public class MaxHybrid extends CompleteOperator<EventBasic> {

    int parallelism;

    int splitParallelism;

    public MaxHybrid(String file, StreamExecutionEnvironment env, int parallelism, int splitParallelism, boolean isJavaSource, int sourceParallelism){
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism);
        this.parallelism = parallelism;
        this.splitParallelism = splitParallelism;
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();


        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = mainStream
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag)).setParallelism(1).name("SwitchNodeEventBasic");

        //basic operator
        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunction()).setParallelism(parallelism).name("basicOperator");

        // time to do the thingy
        DataStream<EventBasic> operatorSplitStream = popularFilterStream.getSideOutput(operatorAggregateTag);

        //how to find the number of partitions before


        DataStream<EventBasic> split = operatorSplitStream
                .partitionCustom(new RoundRobin(splitParallelism), value->value.key ) //any cast
                .process(new MaxPartialFunctionFakeWindowEndEvents(1000)).setParallelism(splitParallelism).name("splitOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MaxFunctionReconcileFakeWindowEndEvents(1000,splitParallelism)).setParallelism(1).name("reconciliationOperator");



//        split.print("split").setParallelism(1);
//        reconciliation.print("reconciliation").setParallelism(1);


        return reconciliation.union(operatorBasicStream);


    }
}
