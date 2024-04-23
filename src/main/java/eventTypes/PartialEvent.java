package eventTypes;

public class PartialEvent {
    /**
     * This class is the basic class for all the events that will be used in the project. key string and value
     */

    public String key;
    public Value value;
    public Object valueTmp;

    public PartialEvent(String key, Value value, int valueTmp){
        this.key = key;
        this.value = value;
        this.valueTmp = valueTmp;
    }

    @Override
    public String toString() {
        return "EventBasic{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }

}
