package zxm.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zmxit on 2015/9/29.
 */
public class WorldCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("world count").setMaster("local[5]");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD textRDD = sc.textFile(args[0]);
        List<String> strs = new ArrayList<String>();
        strs.add("spark world count");
        strs.add("spark world");
        strs.add("spark");

        JavaRDD<String> textRDD = sc.parallelize(strs);
        JavaRDD<String> words = textRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterable call(String str) throws Exception {
                return Arrays.asList(str.split("\\s+"));
            }
        });

        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();

        for(Tuple2<String, Integer> tuple : output) {
            System.out.println(tuple._1() + " : " + tuple._2());
        }

        sc.close();
    }
}
