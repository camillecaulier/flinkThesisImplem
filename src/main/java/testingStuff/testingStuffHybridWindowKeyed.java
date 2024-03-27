package testingStuff;

import keygrouping.RoundRobin;
import keygrouping.SingleCast;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.splitProcessFunction;
import processFunctions.MaxPartialFunction;
import processFunctions.MaxPartialWindowProcessFunction;
import sourceGeneration.RandomStringSource;

public class testingStuffHybridWindowKeyed {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<Tuple2<String, Integer>> strategy = WatermarkStrategy
                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((element, previousTimestamp) -> System.currentTimeMillis());



        DataStream<Tuple2<String, Integer>> mainStream = env
                .addSource(new RandomStringSource())
                .keyBy(tuple -> tuple.f0)
                .assignTimestampsAndWatermarks(strategy);



        DataStream<Tuple2<String, Integer>> processedStream = mainStream
                .keyBy(value-> value.f0);

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


        DataStream<Tuple2<String, Integer>> split = operatorAggregateStream
//                .keyBy(value-> value.f0)
                .partitionCustom(new RoundRobin(), value->value.f0 )
                .process(new MaxPartialFunction());
//                .setParallelism(10);

//        split.setParallelism(2);


        //this is probably not correct as is, since unable to get correct values at the end
        DataStream<Tuple2<String, Integer>> aggregate = split
                .partitionCustom(new SingleCast(), value->value.f0 )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new MaxPartialWindowProcessFunction());


        System.out.println(aggregate.getParallelism());





//        operatorAggregateStream.print("operatorAggregateStream");
//        operatorBasicStream.print("operatorBasicStream");
        split.print("split");

        aggregate.print("aggregate");

        env.execute("Key Group Metric Example");
    }
}
