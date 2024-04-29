package eventTypes;


public class EventBasic {
    /**
     * This class is the basic class for all the events that will be used in the project. key string and value
     */

    public String key;
    public Value value;

    public EventBasic(String key, Value value){
        this.key = key;
        this.value = value;
    }

    public EventBasic(String key, int valueInt, long valueTimeStamp){
        this.key = key;
        this.value = new Value(valueInt, valueTimeStamp);
    }

    public EventBasic(String key, int valueInt, long valueTimeStamp, int valueTmp){
        this.key = key;
        this.value = new Value(valueInt, valueTimeStamp);
    }
    @Override
    public String toString() {
        return "EventBasic{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }

}
