package zxm.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zmxit on 2015/9/29.
 */
public class WorldAdd {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("world count").setMaster("local[4]");
        JavaSparkContext  sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> distData = sc.parallelize(data, 4);

        int totalLength = distData.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        System.out.println(totalLength);

    }
}
