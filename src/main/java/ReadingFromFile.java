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
    // output. One would believe that the foreach is executed onto each partition and then returned.
    //
    // Guessing it intuitively, the first thing that comes to mind is that this can be achieved if
    // we have collect() instead of take(n). But the results will not be sorted as expected.
    // Rather there  will be chunk partitions which are sorted.
    // Okay. This means that this is because the results are obtained asynchronously from different
    // partitions, that the sorting is done on each partition separately and then outputted.
    // So, one might believe that coalescing all partitions on to one partition, using
    // coealesce(1) can solve the problem. And it kind of does too.
    //
    // UTTER BULLSHIT AND WRONG. Most texts, courses and people will say this is the reason.
    // There are so many things wrong here.
    //
    // First, any such coalesce operations will bring data from all partitions, (which in
    // production environment will be on different nodes) onto a single partition (node) and this
    // can cause an out of memory error.
    // Second, in real production environments, sending printing will actually print on that
    // particular node and we won't see the output appear here. Locally, yes there are different
    // threads but on the same JVM. So, we can see the output even though distorted.
    // Third, correctness of a business logic should not be dependent on the number of partitions
    // of Spark, or any performance considerations for that matter. You do not need to shuffle
    // data or need to know the number of partitions for getting the right answer.
    //
    // So, the correct thing to do for correctness is use operations like take or collect. Here,
    // the emphasis is on correctness and not on the performance. Use collect only if you are
    // confident that a single JVM can take that much data.
    // Also, coalesce is used just to reduce the number of partitions when it does not make sense
    // to have an entire partition for a small amount of data.
    sc.close();
  }
}
