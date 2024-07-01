package benchmarks;

public class JavaSourceParameters {
    public String distribution;
    public int windowSize;
    public int numWindow;
    public double skewness;
    public int keySpaceSize;
    /**
     * java class that will take in a string e.g zipfDistribution,250000,2,1,2,0.7 to be used as an input for the java source
     * in the operators
     *zipfDistribution,250000,2,1,2,0.7 => distribution, windowSize, numWindow, keySpaceSize, skewness
     **/

    public JavaSourceParameters(String distribution,int windowSize, int numWindow, int keySpaceSize,double skewness ){
        this.distribution = distribution;
        this.windowSize = windowSize;
        this.numWindow = numWindow;
        this.skewness = skewness;
        this.keySpaceSize = keySpaceSize;
    }

    public static JavaSourceParameters getJavaSourceParameters(String javaSourceParametersString){
        String[] parts = javaSourceParametersString.split(",");
        String distribution = parts[0];
        int windowSize = Integer.parseInt(parts[1]);
        int numWindow = Integer.parseInt(parts[2]);
        int keySpaceSize = Integer.parseInt(parts[3]);
        double skewness = Double.parseDouble(parts[4]);

        return new JavaSourceParameters(distribution, windowSize, numWindow, keySpaceSize,skewness);
    }
}
