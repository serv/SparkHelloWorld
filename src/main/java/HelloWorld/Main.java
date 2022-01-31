package HelloWorld;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		System.out.println("Spark Hello World");

		try (SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate()) {
			Dataset ds = sparkSession.read()
					.format("csv")
					.load("./data/input/seattlePetLicences");
			ds.createOrReplaceTempView("ds");
			ds.selectExpr("_c4 AS breed").write().mode(SaveMode.Overwrite).format("csv").save("./data/output/breeds");
			ds.selectExpr("COUNT(1)").write().mode(SaveMode.Overwrite).format("csv").save("./data/output/count");
			ds.selectExpr("_c4 AS breed").groupBy("breed").count().orderBy(new Column("count").desc())
					.write().mode(SaveMode.Overwrite).format("csv").save("./data/output/count-by-breed");

			System.out.println("Count of rows: " + ds.count());

			sparkSession.cloneSession();
		}
	}
}
