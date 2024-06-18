package benchmarks;

public class BenchmarkParameters {
    int MainParallelism;
    int HybridParallelism;
    int Choices;
    String operator;

    int sourceParallelism;

    public BenchmarkParameters(String operator, int MainParallelism, int HybridParallelism, int Choices, int sourceParallelism){
        this.MainParallelism = MainParallelism;
        this.HybridParallelism = HybridParallelism;
        this.Choices = Choices;
        this.operator = operator;
        this.sourceParallelism = sourceParallelism;
    }

}
