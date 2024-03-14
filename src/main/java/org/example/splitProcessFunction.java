package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class splitProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private final OutputTag<Tuple2<String, Integer>> hotKeyOperatorTag;
    private final OutputTag<Tuple2<String, Integer>> operator2OutputTag;

    public splitProcessFunction(OutputTag<Tuple2<String, Integer>> operator1OutputTag, OutputTag<Tuple2<String, Integer>> operator2OutputTag) {
        this.hotKeyOperatorTag = operator1OutputTag;
        this.operator2OutputTag = operator2OutputTag;
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//        is popular()
        if (value.f0.equals("A") || value.f0.equals("B") || value.f0.equals("C")){
            ctx.output(hotKeyOperatorTag, value); //send to operator 1
        } else {
            ctx.output(operator2OutputTag, value); //send to operator 2
        }
        //TODO send in the output stream the non popular keys

    }



}
