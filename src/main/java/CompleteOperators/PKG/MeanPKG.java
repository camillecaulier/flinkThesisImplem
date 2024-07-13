package CompleteOperators.PKG;

import CompleteOperators.CompleteOperator;
import WatermarkGenerators.periodicWatermarkGenerator;
import eventTypes.EventBasic;
import keygrouping.PKG;
import keygrouping.keyGroupingBasic;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import processFunctions.dummyNode;
import processFunctions.reconciliationFunctionsComplete.MeanFunctionReconcileFakeWindowEndEvents;

public class MeanPKG extends CompleteOperator<EventBasic> {

    private String filePath;
    private final StreamExecutionEnvironment env;
    private final WatermarkStrategy<EventBasic> watermarkStrategy;
    int parallelism;
    boolean isJavaSource;
    public MeanPKG(String file, StreamExecutionEnvironment env, int parallelism, boolean isJavaSource, int sourceParallelism, int aggregatorParallelism) {
        super(file,
                env,
                isJavaSource, sourceParallelism,parallelism, aggregatorParallelism);
        this.filePath = file;
        this.env = env;
        this.watermarkStrategy = WatermarkStrategy
                .<EventBasic>forGenerator(ctx -> new periodicWatermarkGenerator(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.value.timeStamp);
        this.parallelism = parallelism;
        this.isJavaSource = isJavaSource;
    }

//    public DataStream<EventBasic> execute(){
//
//        DataStream<EventBasic> mainStream = createSource();
//
//
//
//        DataStream<EventBasic> split = mainStream
//                .partitionCustom(new PKG(parallelism), value->value.key ) //any cast
//                .process(createPartialFunctions(true)).setParallelism(parallelism).name("PKGOperator");
//
//
//        DataStream<EventBasic> reconciliation = split
//                .process(new MeanFunctionReconcileFakeWindowEndEvents(1000,parallelism)).setParallelism(1).name("reconciliation");
//
//        return reconciliation;
//    }o

    @Override
    public keyGroupingBasic getKeyGrouping() {
        return new PKG(parallelism);
    }
}


