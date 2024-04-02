package KeySelector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyKeySelector<T,KEY> implements KeySelector<Tuple2<String, Integer>, String> {

    @Override
    public String getKey(Tuple2<String, Integer> value) throws Exception {
//        System.out.println("KeySelector: " + value.f0);
        return value.f0;
    }
}