import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class Mapping implements Serializable {
  public static void main(String[] args) {
    final List<Integer> inputData = new ArrayList<>();
    inputData.add(35);
    inputData.add(23);
    inputData.add(453);
    inputData.add(256);
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    final SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    final JavaSparkContext sc = new JavaSparkContext(conf);
    final JavaRDD<Integer> myRdd = sc.parallelize(inputData);

    // reduce operations
    final Integer result = myRdd.reduce(Integer::sum);
    System.out.println(result);

    // map operations
    final JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

    // This will go on all the machines (CPUs on a local machine), gather the data and
    // then printout. This is because Spark cannot send System.out.println to each machine because
    // it is not serializable.
    sqrtRdd.collect().forEach(System.out::println);

    // Getting the count of elements in the RDD
    System.out.println(sqrtRdd.count()); // This gives the count but does not persist that to RDD
    final JavaRDD<Long> singleIntegerRdd = myRdd.map(value -> 1L);
    final Long count = singleIntegerRdd.reduce(Long::sum);
    System.out.println(count + " - " + sqrtRdd.count());

    final Tuple3<Integer, String, Double> tup = new Tuple3<>(1, "asdasd", 2.3D);

    // create a tuple RDD with the integer and its square root inside the tuple
    final JavaRDD<Tuple2<Integer, Double>> sqrtTupleRDD =
      myRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
    sqrtTupleRDD.collect().forEach(System.out::println);
    sc.close();
  }
}
