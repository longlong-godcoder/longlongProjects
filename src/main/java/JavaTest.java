
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaTest {

    public static void main(String[] args) {
        SparkSession javaTest = SparkSession.builder().appName("JavaTest").master("local[4]").getOrCreate();
        Row row1 = RowFactory.create(1, "longlong");
        Row row2 = RowFactory.create(2, "doubi");
        List<Row> rows = new ArrayList<Row>();
        rows.add(row1);
        rows.add(row2);
        SparkContext sparkContext = javaTest.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        JavaRDD<Row> rdd = javaSparkContext.parallelize(rows);
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true)
        ));
        Dataset<Row> dataFrame = javaTest.createDataFrame(rdd, schema);
//        dataFrame.registerTempTable("TestTable");

        dataFrame.createOrReplaceGlobalTempView("hehe");

        Dataset<Row> sql = javaTest.sql("select * from global_temp.hehe");
        sql.show();


    }
}
