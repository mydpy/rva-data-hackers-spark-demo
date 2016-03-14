import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;
import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.*
import org.apache.spark.api.java.function.*

public class WordCount {
  public static void main(String[] args) throws Exception {
    String master = "local";
    JavaSparkContext sc = new JavaSparkContext(
      master, "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
    JavaRDD&#60;String> rdd = sc.textFile("spark-input/boa-constrictor");
    JavaPairRDD&#60;String, Integer> counts = rdd.flatMap(
      new FlatMapFunction&#60;String, String>() {
        public Iterable&#60;String> call(String x) {
          return Arrays.asList(x.split(" "));
        }}).mapToPair(new PairFunction&#60;String, String, Integer>(){
          public Tuple2&#60;String, Integer> call(String x){
            return new Tuple2(x, 1);
          }}).reduceByKey(new Function2&#60;Integer, Integer, Integer>(){
              public Integer call(Integer x, Integer y){ return x+y;}});
  counts.saveAsTextFile("output/boa-constrictor");
  }
}
