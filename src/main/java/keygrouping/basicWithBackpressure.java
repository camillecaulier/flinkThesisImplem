package keygrouping;

import eventTypes.EventBasic;
import org.apache.flink.api.common.functions.Partitioner;

public class basicWithBackpressure implements Partitioner<EventBasic> {

    @Override
    public int partition(EventBasic key, int numPartitions) {
        //a and b highest go to only one
        if(key.key == "a" || key.key == "b"){
            return 0;
        }
        else if( key.key == "c" || key.key == "d"){
            return 1;

        }
        return 0;
    }
}
