package CompleteOperators.Hybrid;


import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import keygrouping.basicHash;
import keygrouping.keyGroupingBasic;
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
    int aggregatorParallelism;

    public MeanHybrid(String file, StreamExecutionEnvironment env,int parallelism, int splitParallelism,boolean isJavaSource, int sourceParallelism,int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism,aggregatorParallelism);

        this.parallelism = parallelism;
        this.splitParallelism = splitParallelism;
        this.aggregatorParallelism = (int) Math.ceil( (double) aggregatorParallelism/2.0);
    }

    @Override
    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};


        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = mainStream
                .process(new SwitchNodeEventBasic(operatorAggregateTag, operatorBasicTag, splitParallelism+ parallelism)).setParallelism(sourceParallelism).name("switchNodeEventBasic");

        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .partitionCustom(new basicHash(splitParallelism), value->value.key ) //any cast
                .process(createPartialFunctions(false)).setParallelism(splitParallelism).name("hashOperator");


        // time to do the thingy
        DataStream<EventBasic> operatorSplitStream = popularFilterStream.getSideOutput(operatorAggregateTag);

        //how to find the number of partitions before
        DataStream<EventBasic> split = operatorSplitStream
                .partitionCustom(new RoundRobin(splitParallelism), value->value.key ) //any cast
                .process(createPartialFunctions(true)).setParallelism(splitParallelism).name("splitOperator");

        split.print("split").setParallelism(1);
        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,splitParallelism)).setParallelism(1).name("reconciliationOperator");



        reconciliation.print("reconciliation").setParallelism(1);


        return reconciliation.union(operatorBasicStream);


    }

    public keyGroupingBasic getKeyGrouping() {
        return new RoundRobin(parallelism);
    }
}
