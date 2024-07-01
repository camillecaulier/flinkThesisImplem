package CompleteOperators.RoundRobin;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import processFunctions.partialFunctions.MaxPartialFunctionFakeWindowEndEvents;
import processFunctions.reconciliationFunctionsComplete.MaxFunctionReconcileFakeWindowEndEvents;
import sourceGeneration.CSVSourceFlatMap;

import java.time.Duration;

public class MaxRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    
    public MaxRoundRobin(String file, StreamExecutionEnvironment env , int parallelism, boolean isJavaSource,int sourceParallelism) {
        super(file,
                env,
                isJavaSource,sourceParallelism,parallelism);

        this.parallelism = parallelism;

    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> operatorBasicStream = mainStream
                .partitionCustom(new RoundRobin(this.parallelism), value->value.key )
                .process(createPartialFunctions(true)).setParallelism(this.parallelism).name("roundRobinOperator");

        DataStream<EventBasic> reconciliation = operatorBasicStream
                .process(new MaxFunctionReconcileFakeWindowEndEvents(1000, this.parallelism)).setParallelism(1).name("reconciliation");

        return reconciliation;
    }
}
