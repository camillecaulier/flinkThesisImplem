package CompleteOperators.AggregateAware;

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
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunction;
import sourceGeneration.CSVSourceParallelized;

import java.time.Duration;

public class MeanAggregateAware {

    private String csvFilePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;
    int parallelism;
    int choices;
    public MeanAggregateAware(String csvFilePath, StreamExecutionEnvironment env, int parallelism , int choices) {
        this.csvFilePath = csvFilePath;
        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(500))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
        this.parallelism = parallelism;
        this.choices = choices;
    }

    public DataStream<EventBasic> execute(int parallelism){
        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_ONCE, 1000).setParallelism(1)
                .flatMap(new CSVSourceParallelized()).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy);



        DataStream<EventBasic> split = mainStream
                .partitionCustom(new cam_n(choices ,parallelism), value->value.key ) //any cast
                .process(new MaxPartialFunctionFakeWindowEndEvents(1000)).setParallelism(parallelism);


        DataStream<EventBasic> reconciliation = split
                .process(new MaxFunctionReconcileFakeWindowEndEvents(1000,5,3)).setParallelism(1);


        return reconciliation;
    }
}
