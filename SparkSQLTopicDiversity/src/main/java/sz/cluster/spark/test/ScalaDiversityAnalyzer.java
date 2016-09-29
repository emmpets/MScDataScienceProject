package sz.cluster.spark.test;

import java.util.Arrays;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class ScalaDiversityAnalyzer {



	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL Example")
				.config("spark.some.config.option", "some-value").master("local[8]").getOrCreate();
		
	

		Dataset<Row> d1 = spark.read().text("inputdirshort/*").toDF();
		Dataset<Row> d2 = spark.read().text("inputdirshort/*").toDF();
	
		
		d1=d1.withColumn("id", functions.monotonically_increasing_id());
		d2=d2.withColumn("id", functions.monotonically_increasing_id());
		//d1.printSchema();
	//System.out.println(Arrays.asList(d1.columns()));
	
		Dataset<Row> joined = d1.toDF().join(d2.toDF(),  (d1.col("id").$greater(d2.col("id"))));
		
	
	
		org.apache.spark.sql.Row rows[]= (Row[]) joined.collect();
		
		for(Row r:rows)
		System.out.println(r.get(0)+" "+r.get(2));
		
		System.out.println(rows.length);
		
	

	}
}