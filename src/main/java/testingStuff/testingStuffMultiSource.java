package testingStuff;

import WatermarkGenerators.periodicWatermarkGenerator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsMultiSource;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.ZipfStringSource;
import sourceGeneration.ZipfStringSourceRichProcessFunction;

import java.time.Duration;

public class testingStuffMultiSource {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        int parallelism = 10;
        int source_parallelism = 3;
//        WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy
//                .<EventBasic>forGenerator(ctx -> new periodicWatermarkGenerator(100000/3))
//                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);

        WatermarkStrategy<EventBasic> watermarkStrategy= WatermarkStrategy.<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(10))
                .withTimestampAssigner((SerializableTimestampAssigner<EventBasic>) (element, recordTimestamp) -> element.value.timeStamp);

//        WatermarkStrategy<EventBasic> watermarkStrategy2= WatermarkStrategy.<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(10))
//                .withTimestampAssigner((SerializableTimestampAssigner<EventBasic>) (element, recordTimestamp) -> element.value.timeStamp);


        int numWindow = 10;
        DataStream<EventBasic> mainStream = env.addSource(new ZipfStringSourceRichProcessFunction(100000,numWindow,2, 1.4, source_parallelism))
                .setParallelism(source_parallelism)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .setParallelism(source_parallelism);

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new RoundRobin(parallelism), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEventsMultiSource(1000, 10,2)).setParallelism(parallelism).name("roundRobinOperator");


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");

        env.execute("Benchmarking operator" );
    }
}
