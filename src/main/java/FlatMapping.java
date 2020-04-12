import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapping {
  public static void main(String[] args) {
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Tuesday 4 September 0405");
    inputData.add("ERROR: Tuesday 4 September 0408");
    inputData.add("FATAL: Wednesday 5 September 1632");
    inputData.add("ERROR: Friday 7 September 1854");
    inputData.add("WARN: Saturday 8 September 1942");

    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> sentences = sc.parallelize(inputData);
    JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//    words.collect().forEach(System.out::println);

    // We can also filter rdd
    words.filter(word -> word.length() > 1).collect().forEach(System.out::println);
    sc.close();
  }
}
