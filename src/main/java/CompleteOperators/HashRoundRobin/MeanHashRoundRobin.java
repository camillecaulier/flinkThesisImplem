package CompleteOperators.HashRoundRobin;

import CompleteOperators.CompleteOperator;
import eventTypes.EventBasic;
import keygrouping.basicHash;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MeanHashRoundRobin extends CompleteOperator<EventBasic> {

    int parallelism;
    public MeanHashRoundRobin(String file, StreamExecutionEnvironment env, int parallelism , boolean isJavaSource, int sourceParallelism) {
        super(file, env, isJavaSource, sourceParallelism,parallelism);
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<EventBasic> execute() {
        DataStream<EventBasic> mainStream = createSource();

        DataStream<EventBasic> split = mainStream
                .partitionCustom(new basicHash(parallelism), value->value.key ) //any cast
                .process(createPartialFunctions(false)).setParallelism(parallelism).name("HashRoundRobinOperator");

        return split;
    }
}
