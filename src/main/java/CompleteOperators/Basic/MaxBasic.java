package CompleteOperators.Basic;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import processFunctions.reconciliationFunctionsComplete.MaxWindowProcessFunction;
import sourceGeneration.CSVSourceFlatMap;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class MaxBasic extends CompleteOperator<EventBasic> {

    int parallelism;
    public MaxBasic(String file, StreamExecutionEnvironment env, int parallelism, boolean isJavaSource,int sourceParallelism) {
        super(file,
                env,
                isJavaSource,sourceParallelism);
        this.parallelism = parallelism;
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> operatorBasicStream = mainStream
                .keyBy(event -> event.key)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new MaxWindowProcessFunction()).setParallelism(parallelism).name("basicOperator");

        return operatorBasicStream;
    }
}
