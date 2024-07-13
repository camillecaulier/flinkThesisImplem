package benchmarks;

public class BenchmarkParameters {
    int MainParallelism;
    int HybridParallelism;
    int Choices;
    String operator;

    int sourceParallelism;
    int aggregatorParallelism;

    public BenchmarkParameters(String operator, int MainParallelism, int HybridParallelism, int Choices, int sourceParallelism, int aggregatorParallelism){
        this.MainParallelism = MainParallelism;
        this.HybridParallelism = HybridParallelism;
        this.Choices = Choices;
        this.operator = operator;
        this.sourceParallelism = sourceParallelism;
        this.aggregatorParallelism = aggregatorParallelism;
    }

}
