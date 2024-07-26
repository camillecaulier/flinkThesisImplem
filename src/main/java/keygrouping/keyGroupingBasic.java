package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;

import StringConstants.StringConstants;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

public abstract class keyGroupingBasic implements   Partitioner<String> {

    int[] primeNumbers = {13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67};

    public float thresholdForTopK;

    public EndWindowPropagation endWindowPropagation;
    public keyGroupingBasic(int parallelism){
        endWindowPropagation = new EndWindowPropagation(parallelism);
        thresholdForTopK = (float) 1 /parallelism;
    }

    @Override
    public int partition(String key, int numPartitions){
        if(key.equals(StringConstants.WINDOW_END)){
            return endWindowPropagation.endWindowRouting();
        }
        return customPartition(key, numPartitions);
    }


    public HashFunction[] createHashFunctions(int n){
        if (n > primeNumbers.length){
            throw new IllegalArgumentException("Number of partitions is too large");
        }
        HashFunction[] hashFunctions = new HashFunction[n];
        for (int i = 0; i < n; i++){
            hashFunctions[i] = Hashing.murmur3_128(primeNumbers[i]);
        }
        return hashFunctions;
    }

    public abstract int customPartition(String key, int numPartitions);

}
