package CompleteOperators.RoundRobin;

import CompleteOperators.CompleteOperator;
import WatermarkGenerators.periodicWatermarkGenerator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

import java.time.Duration;

public class MeanRoundRobin extends CompleteOperator<EventBasic> {

    private String filePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;
    int parallelism;
    boolean isJavaSource;
    public MeanRoundRobin(String file, StreamExecutionEnvironment env, int parallelism, boolean isJavaSource, int sourceParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism);
        this.filePath = file;
        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forGenerator(ctx -> new periodicWatermarkGenerator(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
        this.parallelism = parallelism;
        this.isJavaSource = isJavaSource;
    }

    public DataStream<EventBasic> execute(){
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(10);

//        env.getConfig().enableClosureCleaner()

//        DataStream<EventBasic> mainStream = env.readFile(  new TextInputFormat(new Path(csvFilePath)), csvFilePath, FileProcessingMode.PROCESS_ONCE, 1000)
//                .setParallelism(3)
//                .flatMap(new CSVSourceFlatMapParallelized())
//                .setParallelism(3)
//                .assignTimestampsAndWatermarks(watermarkStrategy).name("source");

        DataStream<EventBasic> mainStream = createSource();




        DataStream<EventBasic> split = mainStream
                .partitionCustom(new RoundRobin(), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000)).setParallelism(parallelism).name("roundRobinOperator");



        DataStream<EventBasic> reconciliation = split
                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");


//        reconciliation.print("reconciliation");
        return reconciliation;
    }

    public static class PrintWatermarkProcessFunction extends ProcessFunction<EventBasic, EventBasic> {

        //        mainStream
//                .process(new PrintWatermarkProcessFunction())
//                .name("Watermark Logger");

        @Override
        public void processElement(EventBasic value, Context ctx, Collector<EventBasic> out) throws Exception {
            out.collect(value);
            // Log the current watermark
            System.out.println("Current Watermark: " + ctx.timerService().currentWatermark());
        }
    }
}
