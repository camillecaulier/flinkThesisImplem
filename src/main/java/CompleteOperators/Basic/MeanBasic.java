package CompleteOperators.Basic;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import processFunctions.reconciliationFunctionsComplete.MeanWindowProcessFunction;
import sourceGeneration.CSVSourceParallelized;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class MeanBasic implements CompleteOperator {
    private String csvFilePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;
    int parallelism;
    public MeanBasic(String csvFilePath, StreamExecutionEnvironment env,int parallelism){
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

        DataStream<EventBasic> operatorBasicStream = mainStream
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MeanWindowProcessFunction()).setParallelism(parallelism);

        return operatorBasicStream;
    }
}
