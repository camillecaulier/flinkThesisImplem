package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class testingStuffNoWindows {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);


        DataStream<Tuple2<String, Integer>> mainStream = env
                .addSource(new RandomStringSource()).keyBy(tuple -> tuple.f0);





        DataStream<Tuple2<String, Integer>> processedStream = mainStream.keyBy(value-> value.f0);
//                .process(new KeyGroupMetricProcessFunction());

//        DataStream<Tuple2<String, Integer>> processedStream = mainStream.keyBy(value-> value.f0)
//                .process(new KeyGroupMetricBroadcastProcessFunction());


        // Create OutputTags for different operators
        OutputTag<Tuple2<String, Integer>> operatorAggregateTag = new OutputTag<Tuple2<String, Integer>>("operatorAggregate"){};
        OutputTag<Tuple2<String, Integer>> operatorBasicTag = new OutputTag<Tuple2<String, Integer>>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<Tuple2<String, Integer>> popularFilterStream = processedStream
                .process(new splitProcessFunction(operatorAggregateTag, operatorBasicTag));


        //basic operator
        DataStream<Tuple2<String, Integer>> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.f0, value.f1 * 10));
                    }
                });



        // time to do the thingy
        DataStream<Tuple2<String, Integer>> operatorAggregateStream = popularFilterStream.getSideOutput(operatorAggregateTag);


        //how to find the number of parittions before

        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream
                .partitionCustom(new RoundRobin(), value->value.f0 ) //any cast
                .process(new MaxPartialFunction());


        DataStream<Tuple2<String, Integer>> reconciliation = aggregation.partitionCustom(new SingleCast(), value->value.f0 ).process(new MaxPartialFunction());


//        operatorAggregateStream.print("operatorAggregateStream");
//        operatorBasicStream.print("operatorBasicStream");
//        popularFilterStream.print("popularFilterStream");
//        aggregation.print("aggregation");
        reconciliation.print("reconciliation");

        env.execute("Key Group Metric Example");
    }
}
