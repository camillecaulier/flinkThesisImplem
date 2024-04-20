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
    @Override
    public String toString() {
        return "EventBasic{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }

}
