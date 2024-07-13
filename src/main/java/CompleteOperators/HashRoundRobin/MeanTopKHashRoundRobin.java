package CompleteOperators.HashRoundRobin;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.HashRoundRobinTopK;
import keygrouping.keyGroupingBasic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

public class MeanTopKHashRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    public MeanTopKHashRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file, env, isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);
        this.parallelism = parallelism;
    }

//    public DataStream<EventBasic> execute() {
//        DataStream<EventBasic> mainStream = createSource();
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new HashRoundRobinTopK(parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("HashRoundRobinTopKOperator");
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new HashRoundRobinTopK(parallelism);
    }
}
