package CustomWindowing;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import static StringConstants.StringConstants.WINDOW_END;
public class Windowing {
    int sourceParallelism;
    int destinationParallelism;
    int subtaskIndex;

    String info;

    public HashMap<Long, HashSet<Integer>> endWindowEventsReceived = new HashMap<>();
    public Windowing(int sourceParallelism, int destinationParallelism, int subtaskIndex){
        this.sourceParallelism = sourceParallelism;
        this.destinationParallelism = destinationParallelism;
        this.subtaskIndex = subtaskIndex;
    }

    public Windowing(int sourceParallelism, int destinationParallelism, int subtaskIndex, String info){
        this.sourceParallelism = sourceParallelism;
        this.destinationParallelism = destinationParallelism;
        this.subtaskIndex = subtaskIndex;
        this.info = info;
    }
    public boolean isEndWindowEvent(EventBasic event){
        return Objects.equals(event.key, WINDOW_END);
    }

    public void outputEndWindow(Collector<EventBasic> out, long timestamp){

        for(int i = 0; i < destinationParallelism; i++){
            out.collect(new EventBasic(WINDOW_END, subtaskIndex, timestamp));
        }

    }


    public void outputEndWindow(SourceFunction.SourceContext<EventBasic> sourceContext, long timestamp) {
        for(int i = 0; i < destinationParallelism; i++){
            sourceContext.collect(new EventBasic(WINDOW_END, subtaskIndex, timestamp));
        }
    }

    public long checkAllEndWindowEventsReceived(EventBasic event) throws Exception{
        if(!Objects.equals(event.key, WINDOW_END)){
            throw  new IllegalArgumentException("Not a window end event");
        }
        if(endWindowEventsReceived.containsKey(event.value.timeStamp)){
            HashSet<Integer> set = endWindowEventsReceived.get(event.value.timeStamp);
            set.add(event.value.valueInt);
            if(set.size() == sourceParallelism){

                return event.value.timeStamp;
            }
        }else{
            HashSet<Integer> set = new HashSet<>();
            set.add(event.value.valueInt);
            endWindowEventsReceived.put(event.value.timeStamp, set);
        }
        return -1;
    }
}
