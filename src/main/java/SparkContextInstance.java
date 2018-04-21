import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Roy on 18/4/21.
 */
public enum SparkContextInstance {
    INSTANCE;

    private JavaSparkContext sparkContext;

    public JavaSparkContext getSparkContext() {
        if (sparkContext == null) {
            SparkConf conf = new SparkConf();
            conf.setAppName("sparkLearning").setMaster("local");
            sparkContext = new JavaSparkContext(conf);
        }
        return sparkContext;
    }
}
