package testingStuff;

import eventTypes.EventBasic;
import keygrouping.RoundRobin;
import keygrouping.SingleCast;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import popularKeySwitch.splitProcessFunction;
import processFunctions.MaxPartialFunctionFakeWindow;
import sourceGeneration.RandomStringSource;

public class testingStuffFakeWindow {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStream<EventBasic> mainStream = env
                .addSource(new RandomStringSource()).keyBy(element -> element.key);





        DataStream<EventBasic> processedStream = mainStream.keyBy(value-> value.f0);
//                .process(new KeyGroupMetricProcessFunction());

//        DataStream<EventBasic> processedStream = mainStream.keyBy(value-> value.f0)
//                .process(new KeyGroupMetricBroadcastProcessFunction());


        // Create OutputTags for different operators
        OutputTag<EventBasic> operatorAggregateTag = new OutputTag<EventBasic>("operatorAggregate"){};
        OutputTag<EventBasic> operatorBasicTag = new OutputTag<EventBasic>("operatorBasic"){};

        //needs to be a singleOuoptutStreamOperator if not you cannot get the side outputs
        SingleOutputStreamOperator<EventBasic> popularFilterStream = processedStream
                .process(new splitProcessFunction(operatorAggregateTag, operatorBasicTag));


        //basic operator
        DataStream<EventBasic> operatorBasicStream = popularFilterStream.getSideOutput(operatorBasicTag)
                .process(new ProcessFunction<EventBasic, EventBasic>() {
                    @Override
                    public void processElement(EventBasic value, Context ctx, Collector<EventBasic> out) throws Exception {
                        out.collect(new Tuple2<>(value.f0, value.f1 * 10));
                    }
                });



        // time to do the thingy
        DataStream<EventBasic> operatorAggregateStream = popularFilterStream.getSideOutput(operatorAggregateTag);


        //how to find the number of parittions before

        DataStream<EventBasic> aggregation = operatorAggregateStream
                .partitionCustom(new RoundRobin(), value->value.f0 ) //any cast
                .process(new MaxPartialFunctionFakeWindow(5));


        DataStream<EventBasic> reconciliation = aggregation
                .partitionCustom(new SingleCast(), value->value.f0 )
                .process(new MaxPartialFunctionFakeWindow(1));


//        operatorAggregateStream.print("operatorAggregateStream");
//        operatorBasicStream.print("operatorBasicStream");
//        popularFilterStream.print("popularFilterStream");
        aggregation.print("aggregation");
        reconciliation.print("reconciliation");

        env.execute("Key Group Metric Example");
    }
}
