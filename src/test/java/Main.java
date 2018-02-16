import org.apache.spark.api.java.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SQLContext.implicits$;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hadoop on 2/8/18.
 */
public class Main {

    public void main(String args[]){

// Open spark session
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("TwitterAnalyticsExample").getOrCreate();

        // Load training data
        Dataset<Row> twitterData = sparkSession.read().format("json").load("");

        UDF1 extractHashtags = new UDF1<String, Map<String, Integer>>() {

            @Override
            public Map<String, Integer> call(String tweet) throws Exception {
                Map<String, Integer> result = new HashMap<>();
                Pattern pattern = Pattern.compile("#\\w*");
                Matcher matcher = pattern.matcher(tweet);
                while (matcher.find()) {
                    result.merge(matcher.group(), 1, (v1, v2) -> v1 + v2);
                }
                return result;
            }
        };


        sparkSession.sqlContext().udf().register("extractHashtags", extractHashtags, DataTypes.StringType);

        twitterData.limit(50).select(callUDF("extractHashtags", col("text"))).show(20);
//        JavaSparkContext sc = new JavaSparkContext();
//        JavaRDD<String> lines = sc.textFile("hdfs://...");
//        JavaRDD<String> words = lines.flatMap(
//                new FlatMapFunction<String, String>() {
//                    public Iterable<String> call(String s) {
//                        return Arrays.asList(s.split(" "));
//                    }
//                }
//        );

    }
}
