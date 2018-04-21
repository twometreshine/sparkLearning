import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Roy on 18/4/21.
 */
public class SparkContextTest {
    private static JavaSparkContext sc;

    @BeforeClass
    public static void initSparkContext() {
        sc = SparkContextInstance.INSTANCE.getSparkContext();
    }

    @Test
    public void testContextInit() {
        JavaSparkContext sc = SparkContextInstance.INSTANCE.getSparkContext();
        Assert.assertEquals("sparkLearning", sc.appName());
    }

    @Test
    public void testSparkRDD() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(list);

        int sum = rdd.map(s -> s + 1).reduce((a, b) -> a + b);
        System.out.println("Sum is: " + sum);
        Assert.assertEquals(20, sum);

    }
}
