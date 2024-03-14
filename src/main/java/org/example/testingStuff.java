package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.Random;

//import KeyGroupMetricProcessFunction;

public class testingStuff {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<Tuple2<String, Integer>> strategy = WatermarkStrategy.
                <Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((event, timestamp) -> event.f1);


        DataStream<Tuple2<String, Integer>> mainStream = env
                .addSource(new RandomStringSource()).keyBy(tuple -> tuple.f0).assignTimestampsAndWatermarks(strategy);




        DataStream<Tuple2<String, Integer>> processedStream = mainStream.keyBy(value-> value.f0);
//                .process(new KeyGroupMetricProcessFunction());

//        DataStream<Tuple2<String, Integer>> processedStream = mainStream.keyBy(value-> value.f0)
//                .process(new KeyGroupMetricBroadcastProcessFunction());


        // Create OutputTags for different operators
        OutputTag<Tuple2<String, Integer>> operatorAggregateTag = new OutputTag<Tuple2<String, Integer>>("operatorAggregate"){};
        OutputTag<Tuple2<String, Integer>> operatorBasicTag = new OutputTag<Tuple2<String, Integer>>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<Tuple2<String, Integer>> splitStream = processedStream
                .process(new splitProcessFunction(operatorAggregateTag, operatorBasicTag));


        //basic operator
        DataStream<Tuple2<String, Integer>> operatorBasicStream = splitStream.getSideOutput(operatorBasicTag)
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value.f0, value.f1 * 10));
            }
        });



        // time to do the thingy
        DataStream<Tuple2<String, Integer>> operatorAggregateStream = splitStream.getSideOutput(operatorAggregateTag);
//                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(new Tuple2<>(value.f0, value.f1 * 100));
//            }
//        });


        //set parallelism will be useful in order to fix the amount of subtasks
//        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream.partitionCustom(new RandomPartitioner(), value->value.f0 )
//                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(new Tuple2<>(value.f0, value.f1 * 10));
//            }
//        });

//        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(new Tuple2<>(value.f0, value.f1 * 10));
//            }
//        });

//        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream.partitionCustom(new RandomPartitioner(), value->value.f0 ).keyBy(value->value.f0)
//                .maxBy(1);

//        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream.partitionCustom(new ShufflePartitioner(), value->value.f0 ).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(new Tuple2<>(value.f0, value.f1 * 10));
//            }
//        });

        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream
                .partitionCustom(new ShufflePartitioner(), value->value.f0 )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MaxPartialWindowProcessFunction());


        //this is probably not correct as is, since unable to get correct values at the end
        DataStream<Tuple2<String, Integer>> reconciliation = aggregation.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MaxPartialWindowProcessFunction());



//                .process(new MaxPartialFunction()).keyBy(value->value.f0).process(new EvalFunction);

//        DataStream<Tuple2<String, Integer>> aggregation = operatorAggregateStream.partitionCustom(new ShufflePartitioner(), value->value.f0 );


//        aggregation.keyBy(value->value.f0).process(new ReconciliationFunction());

        //this is not correct it's for union two STREAMS
//        DataStream<Tuple2<String,Integer>> reconciliation = aggregation.union(operatorBasicStream);
//        reconciliation.print("reconciliation");




//        operatorAggregateStream.print("operatorAggregateStream");
//        operatorBasicStream.print("operatorBasicStream");
        aggregation.print("aggregation");

        reconciliation.print("reconciliation");

        env.execute("Key Group Metric Example");
    }



}
