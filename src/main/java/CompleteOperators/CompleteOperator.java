package CompleteOperators;

import benchmarks.JavaSourceParameters;
import eventTypes.EventBasic;
import keygrouping.cam_roundRobin;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsMultiSource;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionFakeWindowMultiSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionFakeWindowSingleSource;
import sourceGeneration.ZipfStringSource;
import sourceGeneration.ZipfStringSourceRichProcessFunction;
import sourceGeneration.ZipfStringSourceRichProcessFunctionEndWindow;

import java.time.Duration;

import static benchmarks.JavaSourceParameters.getJavaSourceParameters;

public abstract class CompleteOperator<T> {
    public String file;
    public StreamExecutionEnvironment env;

    public boolean isJavaSource;
    public int sourceParallelism;

    public int outOfOrderness = 10;

    public int parallelism;
    public WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy.<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderness))
            .withTimestampAssigner((SerializableTimestampAssigner<EventBasic>) (element, recordTimestamp) -> element.value.timeStamp);

    public CompleteOperator(String file,StreamExecutionEnvironment env, boolean isJavaSource, int sourceParallelism, int parallelism){
        this.env = env;
        this.isJavaSource = isJavaSource;
        this.file = file;
        this.sourceParallelism = sourceParallelism;
        this.parallelism = parallelism;
    }
    public abstract DataStream<T> execute();



    public DataStream<EventBasic> createSource(){
        if (isJavaSource && sourceParallelism == 1){
            System.out.println("using java source");
            JavaSourceParameters parameters = getJavaSourceParameters(file);
            return env.addSource(new ZipfStringSource(parameters.windowSize, parameters.numWindow, parameters.keySpaceSize, parameters.skewness))
                    .setParallelism(1)
                    .assignTimestampsAndWatermarks(watermarkStrategy)
                    .setParallelism(1)
                    .name("source");
        }
        else if(isJavaSource && sourceParallelism > 1){
            System.out.println("using java multi source");
            JavaSourceParameters parameters = getJavaSourceParameters(file);
//            return env.addSource(new ZipfStringSourceRichProcessFunction(parameters.windowSize, parameters.numWindow, parameters.keySpaceSize, parameters.skewness, sourceParallelism))
//                    .setParallelism(sourceParallelism)
//                    .assignTimestampsAndWatermarks(watermarkStrategy)
//                    .setParallelism(sourceParallelism)
//                    .name("source");
            return env.addSource(new ZipfStringSourceRichProcessFunctionEndWindow(parameters.windowSize, parameters.numWindow, parameters.keySpaceSize, parameters.skewness, sourceParallelism,parallelism))
                    .setParallelism(sourceParallelism)
//                    .assignTimestampsAndWatermarks(watermarkStrategy)
//                    .setParallelism(sourceParallelism)
                    .name("source");
        }
        else {
            // create source from csv file
            System.out.println("using csv source");
            return env.readFile(  new TextInputFormat(new Path(file)), file, FileProcessingMode.PROCESS_ONCE, 1000)
                    .setParallelism(1)
                    .map(new MapFunction<String, EventBasic>() {
                        @Override
                        public EventBasic map(String line) throws Exception {
                            String[] parts = line.split(",");
                            if (parts.length == 3) {
                                String key = parts[0];
                                int valueInt = Integer.parseInt(parts[1]);
                                long valueTimeStamp = Long.parseLong(parts[2]);
                                return new EventBasic(key, valueInt, valueTimeStamp);
                            } else {
                                return null;
                            }
                        }
                    }).setParallelism(1).assignTimestampsAndWatermarks(watermarkStrategy).setParallelism(1)
                    .name("source");
        }
    }

    public ProcessFunction<EventBasic, EventBasic> createPartialFunctions(boolean needReconciliation ){
        if(needReconciliation){
            if (sourceParallelism > 1){
//                return new MeanPartialFunctionFakeWindowEndEventsMultiSource(1000, outOfOrderness,1);
                return new MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming(sourceParallelism);
            }
            else{
                return new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000);
            }
        }else{ // DOESN'T NEED RECONCILIATION
            if(sourceParallelism > 1){
                JavaSourceParameters parameters = getJavaSourceParameters(file);
//                return new MeanFunctionFakeWindowMultiSource(1000, outOfOrderness,parameters.numWindow);
                return new MeanFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming(sourceParallelism);
            }

            else{
                return new MeanFunctionFakeWindowSingleSource(1000);
            }
        }

    }

}
