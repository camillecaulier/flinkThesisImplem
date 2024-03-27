package godSaveMeIDontknowWhatThisHas;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;

public class maxProcessOperator extends ProcessOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    public maxProcessOperator(ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> function) {
        super(function);
    }
}
