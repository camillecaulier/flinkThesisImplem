package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SwitchNode extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private final OutputTag<Tuple2<String, Integer>> operatorAggregateOutputTag;
    private final OutputTag<Tuple2<String, Integer>> operatorBasciOutputTag;

    public SwitchNode(OutputTag<Tuple2<String, Integer>> operatorAggregateOutputTag, OutputTag<Tuple2<String, Integer>> operatorBasciOutputTag) {
        this.operatorAggregateOutputTag = operatorAggregateOutputTag;
        this.operatorBasciOutputTag = operatorBasciOutputTag;
    }

    @Override
    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if (stringIntegerTuple2.f0.equals("A")) {
            context.output(operatorAggregateOutputTag, stringIntegerTuple2);
        } else {
            context.output(operatorBasciOutputTag, stringIntegerTuple2);
        }
    }
}
