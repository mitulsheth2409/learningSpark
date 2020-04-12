import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Pairs {
  public static void main(String[] args) {
    final List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Tuesday 4 September 0405");
    inputData.add("ERROR: Tuesday 4 September 0408");
    inputData.add("FATAL: Wednesday 5 September 1632");
    inputData.add("ERROR: Friday 7 September 1854");
    inputData.add("WARN: Saturday 8 September 1942");

    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> myRdd = sc.parallelize(inputData);

    // this will give us a Pair RDD that contains a bag of <String, String>
    // mapToPair takes a PairFunction which is sort of a callable that returns a Tuple2
    JavaPairRDD<String, String> pairRDD = myRdd.mapToPair(rawValue -> {
      final String[] arr = rawValue.split(": ");
      return new Tuple2<>(arr[0], arr[1]);
    });
    pairRDD.collect().forEach(System.out::println);
    System.out.println();

    // Now the problem is if we want to segregate everything and count how many dates are there
    // for each level of logging. We can use group by on the pair rdd and then count

    // Group by the key
    JavaPairRDD<String, Iterable<String>> groupRDD = pairRDD.groupByKey();
    groupRDD.collect().forEach(System.out::println);
    System.out.println();
    groupRDD.foreach(tuple -> System.out.println(tuple._1 + " has " +
                                                   tuple._2.spliterator().getExactSizeIfKnown() +
                                                   " instances using groupby."));

    // The problem with group by is that it returns an RDD with value as Iterable<String>
    // To get a count, an iterator has to traversed and counted. In real life situations, this is
    // not feasible as it forms a serious bottleneck

    System.out.println();

    // So we resort to reduce operations instead of groupby
    JavaPairRDD<String, Long> pairRDD2 = myRdd.mapToPair(rawValue -> {
      final String[] arr = rawValue.split(": ");
      return new Tuple2<>(arr[0], 1L);
    });
    pairRDD2.collect().forEach(System.out::println);
    System.out.println();
    JavaPairRDD<String, Long> sumsRdd = pairRDD2.reduceByKey(Long::sum);
    sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances."));

    System.out.println();

    // All the above can be made compact as a fluent API
    myRdd.mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L))
      .reduceByKey(Long::sum)
      .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances."));
    System.out.println();

    // flatmap and filter function
    JavaRDD<String> words = myRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
    words.collect().forEach(System.out::println);
    System.out.println();

    JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
    words.collect().forEach(System.out::println);

    sc.close();
  }
}
