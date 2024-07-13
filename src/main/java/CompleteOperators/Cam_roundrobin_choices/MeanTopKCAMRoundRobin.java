package CompleteOperators.Cam_roundrobin_choices;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.CAMRoundRobinTopK;
import keygrouping.keyGroupingBasic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

public class MeanTopKCAMRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    int choices;

    public MeanTopKCAMRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , int choices, boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);

        this.choices = choices;
        this.parallelism = parallelism;
    }

//    @Override
//    public DataStream<EventBasic> execute() {
//        DataStream<EventBasic> mainStream = createSource();
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new CAMRoundRobinTopK(choices,parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("CAMRoundRobinTopKOperator");
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new CAMRoundRobinTopK(choices,parallelism);
    }
}
