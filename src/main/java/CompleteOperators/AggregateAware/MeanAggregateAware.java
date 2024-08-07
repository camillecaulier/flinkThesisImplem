package CompleteOperators.AggregateAware;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.dummyNode;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

import java.time.Duration;

public class MeanAggregateAware extends CompleteOperator<EventBasic> {


    int parallelism;
    int choices;
    public MeanAggregateAware(String file, StreamExecutionEnvironment env,int parallelism,  int choices,boolean isJavaSource ,int sourceParallelism , int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism, parallelism, aggregatorParallelism);

        this.parallelism = parallelism;
        this.choices = choices;

    }

//    public DataStream<EventBasic> execute(){
//        DataStream<EventBasic> mainStream = createSource();
//
////        DataStream<EventBasic> dummyStep = mainStream.process(new dummyNode()).setParallelism(1).name("dummyNode");
//
////        DataStream<EventBasic> split = mainStream
////                .partitionCustom(new cam_n(choices ,parallelism), value->value.key ) //any cast
////                .process(createPartialFunctions(false)).setParallelism(parallelism).name("aggregateAwareOperator");
//
////        DataStream<EventBasic> split = mainStream
////                .partitionCustom(new cam_n_external(choices ,parallelism), value->value.key ) //any cast
////                .process(createPartialFunctions(false)).setParallelism(parallelism).name("aggregateAwareOperator");
//
//
////        DataStream<EventBasic> partitioning = mainStream.process(new cam_n_richProcessFunction());
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new cam_n(choices ,parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("aggregateAwareOperator");
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");
//
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new cam_n(choices ,parallelism);
    }
}
