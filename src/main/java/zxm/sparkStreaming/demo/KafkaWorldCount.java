package zxm.sparkStreaming.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zxm on 2015/11/16.
 */
public class KafkaWorldCount {



    public static void main(String[] args) {
        int numThreads = Integer.parseInt(args[0]);
        String[] topics = args[1].split(",");
        String zkQuorum = args[2];
        String groupId = args[3];
        SparkConf conf = new SparkConf().setAppName("world count on kafka");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        for(String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap);

        JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();

    }
}
