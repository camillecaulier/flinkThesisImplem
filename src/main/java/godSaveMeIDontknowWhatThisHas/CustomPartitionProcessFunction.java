package godSaveMeIDontknowWhatThisHas;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.atomic.AtomicInteger;

public class CustomPartitionProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private transient AtomicInteger counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.counter = new AtomicInteger(0);
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        // Round-robin partitioning logic
        int partition = counter.getAndIncrement() % getRuntimeContext().getNumberOfParallelSubtasks();
        out.collect(value); // Emit the value with the partitioning key
        ctx.output(new OutputTag<Tuple2<String, Integer>>("partition-output") {}, value);
    }
}
