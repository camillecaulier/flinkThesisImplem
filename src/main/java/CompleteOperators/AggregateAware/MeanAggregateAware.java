package CompleteOperators.AggregateAware;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.cam_n;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

import java.time.Duration;

public class MeanAggregateAware extends CompleteOperator<EventBasic> {


    int parallelism;
    int choices;
    public MeanAggregateAware(String file, StreamExecutionEnvironment env, int parallelism , int choices,boolean isJavaSource ,int sourceParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism);

        this.parallelism = parallelism;
        this.choices = choices;

    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new cam_n(choices ,parallelism), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000)).setParallelism(parallelism).name("aggregateAwareOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");

//        split.print("split").setParallelism(1);
//        reconciliation.print("reconciliation").setParallelism(1);
        return reconciliation;
    }
}
