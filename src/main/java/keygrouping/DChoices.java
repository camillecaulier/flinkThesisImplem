package keygrouping;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import TopK.PHeadCount;
import TopK.Seed;
import TopK.StreamSummaryHelper;
import org.apache.commons.math3.util.FastMath;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;


public class DChoices extends keyGroupingBasic {
    /*
    * method based on the DChoice algorithm see https://github.com/anisnasir/SLBStorm
     */

//    private List<Integer> targetTasks;

    private  long[] targetTaskStats;

    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    private HashFunction[] hash;
    StreamSummary<String> streamSummary;
    int serversNo;
    Long totalItems;
    private Seed seeds;

    int parallelism;

    public DChoices(int parallelism) {
        super(parallelism);
        this.parallelism = parallelism;
//        this.targetTasks = targetTasks;
        targetTaskStats = new long[parallelism];
        streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
        totalItems = (long) 0;
        serversNo = parallelism;
        seeds = new Seed(serversNo);
        hash = new HashFunction[this.serversNo];
        for (int i=0;i<hash.length;i++) {
            hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
        }
    }

    @Override
    public int customPartition(String key, int numPartitions) {
        List<Integer> boltIds = new ArrayList<Integer>();
        StreamSummaryHelper ssHelper = new StreamSummaryHelper();


        streamSummary.offer(key);
        double epsilon = 0.0001;
        int Choice =2;
        HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,thresholdForTopK,totalItems);
        if(freqList.containsKey(key)) {
//            System.out.println("Key found in the stream summary");
            double pTop = ssHelper.getPTop(streamSummary,this.totalItems);
            PHeadCount pHead = ssHelper.getPHead(streamSummary,thresholdForTopK,this.totalItems);
            double pTail = 1-pHead.probability;
            double n = (double)this.serversNo;
            double n_1_n = (n-1)/n;
            double d = FastMath.round(pTop*this.serversNo);
            double val2,val3,val4,sum1;
            double sum2,value1,value2,value3,value4;
            do{
                //finding sum Head
                val2 = FastMath.pow(n_1_n, pHead.numberOfElements*d); // val2 = ((n-1)/n)^h*d
                val3 = 1-val2;
                val4 = FastMath.pow(val3, 2); //val4 = (1-((n-1)/n)^h*d)^2
                sum1 = pHead.probability + pTail*val4;

                //finding sum1
                value1 = FastMath.pow(n_1_n, d);
                value2 = 1-value1;
                value3 = FastMath.pow(value2, d);
                value4 = FastMath.pow(value2, 2);
                sum2 = pTop+((pHead.probability-pTop)*value3)+(pTail*value4);
                d++;
            }while((d<=this.serversNo) && ((sum1 > (val3+epsilon)) || (sum2 > (value2+epsilon))));
            Choice = (int)d-1;

            //Hash the key accordingly
            int counter = 0;
            int[] choice;
            byte[] b = key.toString().getBytes();

//            int selected;
            if(Choice < this.serversNo) {
                choice = new int[Choice];
                while(counter < Choice) {
                    choice[counter] =  FastMath.abs(hash[counter].hashBytes(b).asInt()%serversNo);
                    counter++;
                }

            }else { // if choice = W then this is just doing W - Choices
                choice = new int[this.serversNo];
                while(counter < this.serversNo) {
                    choice[counter] =  counter;
                    counter++;
                }
            }

            int selected = selectMinChoice(targetTaskStats,choice);
            targetTaskStats[selected]++;
            totalItems++;
            return selected;
        }else {
//            System.out.println("Key not found in the stream summary");
            int firstChoice = (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
            int secondChoice = (int) (FastMath.abs(h2.hashBytes(key.getBytes()).asLong()) % numPartitions);
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;

            targetTaskStats[selected]++;
            totalItems++;
            return selected;


        }
    }

    int selectMinChoice(long loadVector[], int choice[]) {
        int index = choice[0];
        for(int i = 0; i< choice.length; i++) {
            if (loadVector[choice[i]]<loadVector[index])
                index = choice[i];
        }
        return index;
    }

    int selectMinChoice(long loadVector[]) {
        int index =0;
        for(int i = 0; i< this.parallelism; i++) {
            if (loadVector[i]<loadVector[index])
                index = i;
        }
        return index;
    }
}
