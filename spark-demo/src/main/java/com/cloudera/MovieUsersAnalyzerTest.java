package com.cloudera;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class MovieUsersAnalyzerTest {


    public static void main(String[] args) {
        /**
         * 创建 Spark 集群上下文 sc ,在 sc 中可以进行各种依赖和参数的设置等，大家可以
         * 通过 SparkSubmit 脚本的 help 去看设置信息
         */
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[4]")
                .setAppName("Movie_Users_Analyzer"));
        //数据存放的目录；
        String dataPath = "file:\\\\\\E:\\CDHProjectDemo\\spark-demo\\data\\moviedata\\medium\\";
        JavaRDD<String> lines = sc.textFile(dataPath + "dataforsecondarysorting.txt");
        JavaPairRDD<SecondarySortingKey, String> keyvalues = lines.mapToPair(
                new PairFunction<String, SecondarySortingKey, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<SecondarySortingKey, String> call(String line) throws Exception {
                        String[] splited = line.split(" ");
                        SecondarySortingKey key =
                                new SecondarySortingKey(Integer.valueOf(splited[0]), Integer.valueOf(splited[1]));
                        // 组合成 Key
                        return new Tuple2<SecondarySortingKey, String>(key, line);
                    }
                });

        // Key 值进行二次排序
        JavaPairRDD<SecondarySortingKey, String> sorted = keyvalues.sortByKey(false);
        JavaRDD<String> result = sorted.map(new Function<Tuple2<SecondarySortingKey, String>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
                // 取第二个值 Value
                return tuple._2;
            }
        });
        List<String> collected = result.take(10);
        for (String item : collected) {
            // 打印输出二次排序后的结果
            System.out.println(item);
        }
    }
}
