package keygrouping;

public class HashRoundRobin extends keyGroupingBasic {
    int index = 0;

    public HashRoundRobin(int parallelism) {
        super(parallelism);
    }
    @Override
    public int customPartition(String key, int numPartitions) {
        if(key.equals("A") || key.equals("B") || key.equals("C")){
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
