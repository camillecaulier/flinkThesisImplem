package godSaveMeIDontknowWhatThisHas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Tree {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a sample stream of Tuple2, where the first element is the key
        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
                new Tuple2<>("key1", 1),
                new Tuple2<>("key2", 2),
                new Tuple2<>("key1", 3),
                new Tuple2<>("key2", 4)
        );

        // keyed stream
        DataStream<Tuple2<String, Integer>> keyedStream = stream.keyBy(value -> value.f0);


        DataStream<Integer> operator1Stream = keyedStream
                .filter(value -> value.f0.equals("key1"))
                .map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String, Integer> value) throws Exception {
                        return value.f1 * 100;
                    }
                });

        DataStream<Integer> operator2Stream = keyedStream
                .filter(value -> value.f0.equals("key2"))
                .map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String, Integer> value) throws Exception {
                        return value.f1 * 300;
                    }
                });

        // Print the results
        operator1Stream.print();
        operator2Stream.print();


        env.execute("Key Group Split Example");

    }
}
