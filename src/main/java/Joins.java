import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Joins {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        final SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Tuple2<Integer, Integer>> leftTable = new ArrayList<>();
        leftTable.add(new Tuple2<>(1, 18));
        leftTable.add(new Tuple2<>(2, 4));
        leftTable.add(new Tuple2<>(10, 9));

        final List<Tuple2<Integer, String>> rightTable = new ArrayList<>();
        rightTable.add(new Tuple2<>(1, "Mitul"));
        rightTable.add(new Tuple2<>(2, "Zarna"));
        rightTable.add(new Tuple2<>(3, "Parin"));
        rightTable.add(new Tuple2<>(4, "Chirag"));
        rightTable.add(new Tuple2<>(5, "Sagar"));
        rightTable.add(new Tuple2<>(6, "Sahil"));

        final JavaPairRDD<Integer, Integer> leftRDD = sc.parallelizePairs(leftTable);
        final JavaPairRDD<Integer, String> rightRDD = sc.parallelizePairs(rightTable);

        // inner join
        System.out.println("Inner join");
        final JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = leftRDD.join(rightRDD);
        joinRDD.collect().forEach(System.out::println);
        System.out.println();

        // left join
        System.out.println("Left join");
        final JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinRDD =
          leftRDD.leftOuterJoin(rightRDD);
        leftJoinRDD.collect().forEach(System.out::println);
        System.out.println();

        // right join
        System.out.println("Right join");
        final JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinRDD =
          leftRDD.rightOuterJoin(rightRDD);
        rightJoinRDD.collect().forEach(System.out::println);
        System.out.println();

        // full join
        System.out.println("Full outer join");
        final JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinRDD =
          leftRDD.fullOuterJoin(rightRDD);
        fullJoinRDD.collect().forEach(System.out::println);
        System.out.println();

        // cross join
        System.out.println("Cross join");
        final JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> crossJoinRDD =
          leftRDD.cartesian(rightRDD);
        crossJoinRDD.collect().forEach(System.out::println);
        System.out.println();

        sc.close();
    }
}
