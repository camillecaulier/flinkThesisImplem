package CompleteOperators.Hybrid;


import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import keygrouping.basicHash;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.SwitchNodeEventBasic;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MeanWindowProcessFunction;

import java.time.Duration;

public class MeanHybrid extends CompleteOperator<EventBasic> {


    int parallelism;

    int splitParallelism;

    public MeanHybrid(String file, StreamExecutionEnvironment env,int parallelism, int splitParallelism,boolean isJavaSource, int sourceParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism);

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
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag)).setParallelism(1).name("switchNodeEventBasic");

        //basic operator
//        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
//                .keyBy(event -> event.key)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
//                .process(new MeanWindowProcessFunction()).setParallelism(parallelism).name("basicOperator");

        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .partitionCustom(new basicHash(), value->value.key ) //any cast
                .process(createPartialFunctions(false)).setParallelism(splitParallelism).name("hashOperator");




        // time to do the thingy
        DataStream<EventBasic> operatorSplitStream = popularFilterStream.getSideOutput(operatorAggregateTag);

        //how to find the number of partitions before
        DataStream<EventBasic> split = operatorSplitStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(createPartialFunctions(true)).setParallelism(splitParallelism).name("splitOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,splitParallelism)).setParallelism(1).name("reconciliationOperator");



//        reconciliation.print("reconciliation").setParallelism(1);


        return reconciliation.union(operatorBasicStream);


    }
}
