package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Objects;

public class basicHash implements Partitioner<String> {
    int index = 0;
    @Override
    public int partition(String key, int numPartitions) {
        if(Objects.equals(key, "ENDD")){
            return roundRobin(numPartitions);
        }
        int hash = key.hashCode();
        return Math.abs(hash % numPartitions);
    }

    public int roundRobin(int numPartitions){
        index++;
        return Math.abs(index % numPartitions);
    }
}
