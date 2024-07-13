package CompleteOperators.Cam_roundrobin_choices;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.CAMRoundRobin;
import keygrouping.keyGroupingBasic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

public class MeanCAMRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    int choices;

    public MeanCAMRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , int choices, boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);

        this.choices = choices;
        this.parallelism = parallelism;
    }

//    public DataStream<EventBasic> execute(){
//        DataStream<EventBasic> mainStream = createSource();
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new CAMRoundRobin(choices ,parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("camOperator");
//
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new CAMRoundRobin(choices ,parallelism);
    }
}
