package CompleteOperators.RoundRobin;

import CompleteOperators.CompleteOperator;
import WatermarkGenerators.periodicWatermarkGenerator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import keygrouping.keyGroupingBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import processFunctions.dummyNode;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

import java.time.Duration;

public class MeanRoundRobin extends CompleteOperator<EventBasic> {
    int parallelism;
    boolean isJavaSource;
    public MeanRoundRobin(String file, StreamExecutionEnvironment env, int parallelism, boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);

        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forGenerator(ctx -> new periodicWatermarkGenerator(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
        this.parallelism = parallelism;
        this.isJavaSource = isJavaSource;
    }

//    public DataStream<EventBasic> execute(){
//
//        DataStream<EventBasic> mainStream = createSource();
//
////        DataStream<EventBasic> dummy = mainStream.process(new dummyNode()).setParallelism(1);
//
//
//
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new RoundRobin(parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("roundRobinOperator");
//
//
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new RoundRobin(parallelism);
    }
}
