package benchmarks;

public class BenchmarkParameters {
    int MainParallelism;
    int HybridParallelism;
    int Choices;
    String operator;

    public BenchmarkParameters(String operator, int MainParallelism, int HybridParallelism, int Choices ){
        this.MainParallelism = MainParallelism;
        this.HybridParallelism = HybridParallelism;
        this.Choices = Choices;
        this.operator = operator;
    }

}
