package testingStuff;

import eventTypes.EventBasic;
import keygrouping.basicNoBackpressure;
import keygrouping.basicWithBackpressure;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.Prototypes.basicProcessFunction;
import sourceGeneration.BasicDistributionSource;

import java.time.Duration;

public class testingStuffBasic {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<EventBasic> source = env.addSource(new BasicDistributionSource())
                .setParallelism(1)
                .name("source");


        DataStream<EventBasic> split = source
                .process(new basicProcessFunction()).setParallelism(2).name("process");




        env.execute("Benchmarking operator" );
    }
}
