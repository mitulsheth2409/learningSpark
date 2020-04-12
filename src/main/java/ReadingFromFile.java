import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.virtualpairprogrammers.Util;

import scala.Tuple2;

public class ReadingFromFile {
  public static void main(String[] args) {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
    System.out.println("This RDD has " + initialRdd.getNumPartitions() + " partitions");
    initialRdd
      .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s+]","").toLowerCase())
      .filter(sentence -> sentence.trim().length() > 0)
      .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
      .filter(Util::isNotBoring)
      .mapToPair(value -> new Tuple2<>(value, 1L))
      .reduceByKey(Long::sum)
      .filter(tuple -> !tuple._1.isEmpty())
      .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
      .sortByKey(false)
      .take(10)
      .forEach(System.out::println);
    // doing foreach here directly after sortByKey won't work here, it does not give the desired
    // output
    // one would believe that the foreach is executed onto each partition and then returned
    // So, one might believe that coalescing all partitions on to one partition can solve the
    // problem.
    // Not true, coalescing onto one partition can result in out of memory error.
    sc.close();
  }
}
