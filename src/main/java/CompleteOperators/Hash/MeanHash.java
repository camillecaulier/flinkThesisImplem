package CompleteOperators.Hash;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.basicHash;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;

import java.time.Duration;

public class MeanHash extends CompleteOperator<EventBasic> {
    int parallelism;
    int choices;
    public MeanHash(String file, StreamExecutionEnvironment env, int parallelism , boolean isJavaSource, int sourceParallelism) {
        super(file,
                env,
                isJavaSource,sourceParallelism);

        this.parallelism = parallelism;

    }

    public DataStream<EventBasic> execute(){
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new basicHash(), value->value.key ) //any cast
                .process(new MeanPartialFunctionFakeWindowEndEventsSingleSource(1000)).setParallelism(parallelism).name("HashOperator");

        // there is no need for a reconciliation function in this case

//        split.print("split").setParallelism(1);
//        reconciliation.print("reconciliation").setParallelism(1);
        return split;
    }
}
