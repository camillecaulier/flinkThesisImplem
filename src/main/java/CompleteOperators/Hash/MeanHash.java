package CompleteOperators.Hash;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.basicHash;
import keygrouping.keyGroupingBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.partialFunctions.MeanPartialFunctionFakeWindowEndEventsSingleSource;

import java.time.Duration;

public class MeanHash extends CompleteOperator<EventBasic> {
    int parallelism;

    public MeanHash(String file, StreamExecutionEnvironment env, int parallelism , boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource,sourceParallelism,parallelism, aggregatorParallelism);

        this.parallelism = parallelism;

    }

//    public DataStream<EventBasic> execute(){
//        DataStream<EventBasic> mainStream = createSource();
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new basicHash(parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(false)).setParallelism(parallelism).name("HashOperator");
//
//        // there is no need for a reconciliation function in this case
//
////        split.print("split").setParallelism(1);
////        reconciliation.print("reconciliation").setParallelism(1);
//        return split;
//    }

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new basicHash(parallelism);
    }
}
