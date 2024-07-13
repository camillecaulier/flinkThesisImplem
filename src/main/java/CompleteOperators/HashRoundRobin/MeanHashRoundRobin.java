package CompleteOperators.HashRoundRobin;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.HashRoundRobin;
import keygrouping.basicHash;
import keygrouping.keyGroupingBasic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

public class MeanHashRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    public MeanHashRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file, env, isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);
        this.parallelism = parallelism;
    }

//    @Override
//    public DataStream<EventBasic> execute() {
//        DataStream<EventBasic> mainStream = createSource();
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new HashRoundRobin(parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("HashRoundRobinOperator");
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new HashRoundRobin(parallelism);
    }
}
