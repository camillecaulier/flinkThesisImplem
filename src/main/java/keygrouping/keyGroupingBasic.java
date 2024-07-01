package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;

import StringConstants.StringConstants;
public abstract class keyGroupingBasic implements   Partitioner<String> {
    public EndWindowPropagation endWindowPropagation;
    public keyGroupingBasic(int parallelism){
        endWindowPropagation = new EndWindowPropagation(parallelism);

    }

    @Override
    public int partition(String key, int numPartitions){
        if(key.equals(StringConstants.WINDOW_END)){
            return endWindowPropagation.endWindowRouting();
        }
        return customPartition(key, numPartitions);
    }


    public abstract int customPartition(String key, int numPartitions);

}
