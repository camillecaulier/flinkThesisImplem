package CompleteOperators;

import benchmarks.JavaSourceParameters;
import eventTypes.EventBasic;
import keygrouping.basicHash;
import keygrouping.cam_n;
import keygrouping.keyGroupingBasic;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionFakeWindowSingleSource;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.ZipfStringSource;
import sourceGeneration.ZipfStringSourceRichProcessFunctionEndWindow;
import sourceGeneration.lowThenHighSkew;

import java.time.Duration;

import static benchmarks.JavaSourceParameters.getJavaSourceParameters;

public abstract class CompleteOperator<T> {
    public String file;
    public StreamExecutionEnvironment env;

    public boolean isJavaSource;
    public int sourceParallelism;

    public int outOfOrderness = 10;

    public int partialFunctionParallelism;

    public int aggregatorParallelism;

    public keyGroupingBasic customKeyGrouping;
    public WatermarkStrategy<EventBasic> watermarkStrategy = WatermarkStrategy.<EventBasic>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderness))
            .withTimestampAssigner((SerializableTimestampAssigner<EventBasic>) (element, recordTimestamp) -> element.value.timeStamp);

    public CompleteOperator(String file, StreamExecutionEnvironment env, boolean isJavaSource, int sourceParallelism, int partialFunctionParallelism, int aggregatorParallelism){
        this.env = env;
        this.isJavaSource = isJavaSource;
        this.file = file;
        this.sourceParallelism = sourceParallelism;
        this.partialFunctionParallelism = partialFunctionParallelism;
        this.aggregatorParallelism = aggregatorParallelism;
    }
    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();


        DataStream<EventBasic> split = mainStream
                .partitionCustom(getKeyGrouping() , value->value.key ) //any cast
                .process(createPartialFunctions(aggregatorParallelism >= 1)).setParallelism(this.partialFunctionParallelism).name("PartialFunctionOperator");


        if (aggregatorParallelism == 1){
            DataStream<EventBasic> reconciliation = split
                    .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,this.partialFunctionParallelism)).setParallelism(1).name("aggregator");

            return reconciliation;
        }
        else if(aggregatorParallelism > 1){
            DataStream<EventBasic> reconciliation = split.partitionCustom(new basicHash(this.aggregatorParallelism), value->value.key)
                    .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,this.partialFunctionParallelism)).setParallelism(aggregatorParallelism).name("aggregator");

            return reconciliation;
        }

//        if (aggregatorParallelism >= 1){
//            DataStream<EventBasic> reconciliation = split
//                    .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,this.partialFunctionParallelism)).setParallelism(1).name("aggregator");
//
//            return reconciliation;
//        }


        return split;
    }

    public abstract keyGroupingBasic getKeyGrouping();


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
            if(parameters.distribution.equals("zipf")){
                return env.addSource(new ZipfStringSourceRichProcessFunctionEndWindow(parameters.windowSize, parameters.numWindow, parameters.keySpaceSize, parameters.skewness, sourceParallelism, partialFunctionParallelism))
                        .setParallelism(sourceParallelism)
                        .name("source");
            }
            else if(parameters.distribution.equals("lowThenHighSkew")){
                return env.addSource(new lowThenHighSkew(parameters.windowSize, parameters.numWindow, sourceParallelism, partialFunctionParallelism))
                        .setParallelism(sourceParallelism)
                        .name("source");

            }
            else{
                return null;
            }

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
                //the latest
                return new MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming(sourceParallelism, aggregatorParallelism);
            }
            else{
                return new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000);
            }
        }else{ // DOESN'T NEED RECONCILIATION
            if(sourceParallelism > 1){
                JavaSourceParameters parameters = getJavaSourceParameters(file);
//                return new MeanFunctionFakeWindowMultiSource(1000, outOfOrderness,parameters.numWindow);
//                System.out.println("hello");
                return new MeanFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming(sourceParallelism);
            }

            else{
                return new MeanFunctionFakeWindowSingleSource(1000);
            }
        }

    }

}
