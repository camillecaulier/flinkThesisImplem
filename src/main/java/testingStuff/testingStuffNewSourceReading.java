package testingStuff;

import eventTypes.EventBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import sourceGeneration.CSVSourceParallelized;



import java.time.Duration;


public class testingStuffNewSourceReading {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        int windowSize = 1;

        WatermarkStrategy<EventBasic> strategy = WatermarkStrategy.
                <EventBasic>forBoundedOutOfOrderness(Duration.ofSeconds(windowSize)).withTimestampAssigner(
                (element, record) -> element.value.timeStamp
        );

        String csvFilePath = "zipf_distribution100_5.csv";

        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
                .flatMap(new CSVSourceParallelized()).assignTimestampsAndWatermarks(strategy).keyBy(element -> element.key);

        //        DataStream<EventBasic> mainStream = env.readFile(
//                        new TextInputFormat(new org.apache.flink.core.fs.Path(csvFilePath)),
//                        csvFilePath,
//                        FileProcessingMode.PROCESS_CONTINUOUSLY,
//                        1000)
//                .keyBy(filePath -> filePath)
//                .flatMap(new CSVSourceParallelized())
//                .keyBy(filePath -> 0);

        mainStream.print("mainStream");

        env.execute("bob");
    }
}



