package CompleteOperators.AggregateAware;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import keygrouping.cam_n;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindowEndEvents;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunction;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.CSVSourceParallelized;

import java.time.Duration;

public class MeanRoundRobin implements CompleteOperator {

    private String csvFilePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;
    int parallelism;
    public MeanRoundRobin(String csvFilePath, StreamExecutionEnvironment env, int parallelism ) {
        this.csvFilePath = csvFilePath;
        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(500))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
        this.parallelism = parallelism;
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_ONCE, 1000).setParallelism(1)
                .flatMap(new CSVSourceParallelized()).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy);



        DataStream<EventBasic> split = mainStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEvents(1000)).setParallelism(parallelism);


        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1);

//        reconciliation.print("reconciliation");
        return reconciliation;
    }
}
