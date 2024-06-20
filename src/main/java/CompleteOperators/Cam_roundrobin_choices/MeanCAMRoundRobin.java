package CompleteOperators.Cam_roundrobin_choices;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.cam_roundRobin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

import java.time.Duration;

public class MeanCAMRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    int choices;

    public MeanCAMRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , int choices, boolean isJavaSource, int sourceParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism);

        this.choices = choices;
        this.parallelism = parallelism;
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new cam_roundRobin(choices ,parallelism), value->value.key ) //any cast
                .process(createPartialFunctions(true)).setParallelism(parallelism).name("camdOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");

        return reconciliation;
    }
}
