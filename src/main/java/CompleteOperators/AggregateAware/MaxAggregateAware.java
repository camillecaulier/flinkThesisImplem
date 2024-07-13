package CompleteOperators.AggregateAware;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.cam_n;
import keygrouping.keyGroupingBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.CSVSourceFlatMap;

import java.time.Duration;

public class MaxAggregateAware extends CompleteOperator<EventBasic> {


    int parallelism;
    int choices;
    public MaxAggregateAware(String file, StreamExecutionEnvironment env, int splitParallelism , int choices, boolean isJavaSource, int sourceParallelism, int aggregatorParallelism){
        super(file,
                env,
                isJavaSource, sourceParallelism,splitParallelism*2, aggregatorParallelism);


        this.parallelism = splitParallelism;
        this.choices = choices;
    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> operatorBasicStream = mainStream
                .partitionCustom(new cam_n(choices ,parallelism), value->value.key )
                .process(new MaxPartialFunctionFakeWindowEndEvents(1000)).setParallelism(this.parallelism).name("aggregateAwareOperator");

        DataStream<EventBasic> reconciliation = operatorBasicStream
                .process(new MaxFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");

        return reconciliation;
    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new cam_n(choices, parallelism);
    }
}
