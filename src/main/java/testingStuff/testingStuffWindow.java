package testingStuff;

import keygrouping.CustomStreamPartitioner;
import keygrouping.RoundRobin;
import keygrouping.SingleCast;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import popularKeySwitch.splitProcessFunction;
import processFunctions.MaxPartialWindowAllProcessFunction;
import processFunctions.MaxWindowProcessFunction;
import sourceGeneration.RandomStringSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;


import KeySelector.MyKeySelector;
import java.lang.reflect.Constructor;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment;


//import static org.apache.flink.api.java.ClosureCleaner.clean;

//import KeyGroupMetricProcessFunction;

public class testingStuffWindow {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = getExecutionEnvironment();
        env.setParallelism(4);


        WatermarkStrategy<Tuple2<String, Integer>> strategy = WatermarkStrategy
                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((element, previousTimestamp) -> System.currentTimeMillis());


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
        SingleOutputStreamOperator<Tuple2<String, Integer>> switchStream = processedStream
                .process(new splitProcessFunction(operatorAggregateTag, operatorBasicTag));


        //basic operator
        DataStream<Tuple2<String, Integer>> operatorBasicStream = switchStream.getSideOutput(operatorBasicTag)
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value.f0, value.f1 * 10));
            }
        });



        // time to do the thingy
        DataStream<Tuple2<String, Integer>> operatorAggregateStream = switchStream
                .getSideOutput(operatorAggregateTag);


        Constructor<KeyedStream> constructor;

        constructor = KeyedStream.class.getDeclaredConstructor(DataStream.class, PartitionTransformation.class, KeySelector.class, TypeInformation.class);

        constructor.setAccessible(true);

        KeySelector<Tuple2<String,Integer>,String> MyKeySelector = new MyKeySelector<Tuple2<String,Integer>,String>();

        PartitionTransformation<Tuple2<String, Integer>> partitionTransformation = new PartitionTransformation<>(
                operatorAggregateStream.getTransformation(),
                new CustomStreamPartitioner(new RoundRobin(), operatorAggregateStream.getExecutionEnvironment().clean(MyKeySelector),10));

//        new PartitionTransformation<>(
//                operatorAggregateStream.getTransformation(),
//                new KeyGroupStreamPartitioner<>(
//                        MyKeySelector,
//                        10)), //also can be

//        KeyGroupRangeAssignment.setDefaultKeyGroupRange(5);

        KeyedStream<Tuple2<String,Integer>,String > keyedStream = constructor.newInstance(operatorAggregateStream,
                partitionTransformation,
                operatorAggregateStream.getExecutionEnvironment().clean(MyKeySelector),
                TypeExtractor.getKeySelectorTypes(MyKeySelector, operatorAggregateStream.getType()));

        keyedStream.print("keyedStream");
        SingleOutputStreamOperator<Tuple2<String,Integer>> split = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MaxWindowProcessFunction()).setParallelism(4);

//        DataStream<Tuple2<String, Integer>> split = operatorAggregateStream
//                .partitionCustom( new RoundRobin(), value->value.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new MaxWindowProcessFunction());

//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new MaxWindowProcessFunction()).setParallelism(10);
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .process(new MaxPartialWindowAllProcessFunction());




        DataStream<Tuple2<String, Integer>> aggregate = split
                .keyBy(value-> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MaxWindowProcessFunction()).setParallelism(1);





//        operatorAggregateStream.print("operatorAggregateStream");
//        operatorBasicStream.print("operatorBasicStream");
//        operatorAggregateStream.print("split");
//        split.print("split");
//        aggregate.print("aggregate");


//        reconciliation.print("reconciliation");

        env.execute("Key Group Metric Example");
    }



}
