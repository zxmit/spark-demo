package zxm.sparkStreaming.demo;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by zxm on 2015/11/16.
 */
public class DirectKafkaWorldCount {

    public static void main(String[] args) {
        String brokers = args[0];
        String[] topics = args[1].split(",");

        SparkConf conf = new SparkConf().setAppName("world count on kafka");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
// Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();
            }
        });

//        lines.foreachRDD(new Function<JavaRDD<String>, Void>(){
//
//            @Override
//            public Void call(JavaRDD<String> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//                    @Override
//                    public void call(Iterator<String> si) throws Exception {
//                        while(si.hasNext()) {
//                            String str = si.next();
//                            System.out.println("====================  " + str);
//                        }
//                    }
//                });
//                return null;
//            }
//        });

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
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
