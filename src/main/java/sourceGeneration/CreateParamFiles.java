package sourceGeneration;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class CreateParamFiles {
    /**
     *  public ZipfStringSource(int windowSize, int numWindow, double skewness, int keySpaceSize){
     *
     */
    public static void writeParams(String distribution,int windowSize, int numWindow, int[] keySpaceSize, double[] skewness) throws IOException {
        String filename = "param_"+windowSize+"_"+numWindow + ".csv";
        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader("Distribution", "WindowSize", "NumWindow", "KeySpaceSize","Skewness");

        try (Writer writer = new FileWriter(filename); CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)){
            for(int keySize : keySpaceSize){
                for(double skew : skewness){
                    csvPrinter.printRecord(distribution, windowSize, numWindow, keySize, skew);
                }
            }

            System.out.println("params written to file: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        writeParams("zipf", 100000, 50,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
//        writeParams("zipf", 100000, 400,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
        writeParams("zipf", 100000, 360,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
        writeParams("zipf", 400000, 90,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
//        writeParams("zipf", 100000, 1000,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
//        writeParams("zipf", 400000, 300,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});
//        writeParams("zipf", 100000, 4000,  new int[]{1,2,3},new double[]{1.0E-15,0.7, 1.4,2.1});


    }
}
