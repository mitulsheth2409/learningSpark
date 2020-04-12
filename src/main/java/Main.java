import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
  public static void main(String[] args) {
    List<Double> inputData = new ArrayList<>();
    inputData.add(1.2);
    inputData.add(2.3);
    inputData.add(4D);
    inputData.add(3.5);
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    // This object is passed to the Spark where it is cloned and can no longer be modified
    // This means that Spark does not support modifying the configuration at runtime
    // Also, local[*] means use all the resources from the local machine
    SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    // Only one spark context per JVM. We have to stop the context before creating a new one
    // This limitation might be eventually removed
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Double> myRdd = sc.parallelize(inputData);
    JavaDoubleRDD sRdd = sc.parallelizeDoubles(inputData);
    double result = myRdd.reduce(Double::sum);
    double res = sRdd.reduce(Double::sum);
    System.out.println(result);
    System.out.println(res);
    System.out.println(inputData.stream().reduce(Double::sum));
    sc.close();
  }
}
