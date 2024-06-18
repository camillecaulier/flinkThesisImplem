package testingStuff;

import WatermarkGenerators.periodicWatermarkGenerator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.ZipfStringSource;

public class testingStuffTryingToGetTheMetricsWithJavaSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        int parallelism = 10;
        WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy
                .<EventBasic>forGenerator(ctx -> new periodicWatermarkGenerator(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);

        DataStream<EventBasic> mainStream = env.addSource(new ZipfStringSource(100000,50,2, 1.4))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .setParallelism(1);

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000)).setParallelism(parallelism).name("roundRobinOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");

        env.execute("Benchmarking operator" );
    }
}
