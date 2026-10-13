import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object ECommercePipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 29 - End-to-End E-Commerce Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("\n========== 1. RAW ORDERS ==========")

    val rawOrders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/orders.csv")

    rawOrders.show(false)

    // CLEAN: validate required fields and business rules.
    val cleanOrders = rawOrders
      .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn("unit_price", col("unit_price").cast("double"))
      .filter(
        col("order_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("order_date").isNotNull &&
        col("quantity") > 0 &&
        col("unit_price") > 0
      )
      .dropDuplicates("order_id")
      .withColumn("order_amount", col("quantity") * col("unit_price"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("\n========== 2. CLEAN ORDERS ==========")
    cleanOrders.show(false)

    // Load small product reference dataset.
    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/products.csv")
      .withColumn("reference_price", col("reference_price").cast("double"))

    // ENRICH: use a broadcast join for the small reference table.
    val enrichedOrders = cleanOrders
      .join(broadcast(products), Seq("product_id"), "inner")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("\n========== 3. ENRICHED ORDERS ==========")
    enrichedOrders.show(false)

    // RDD / PAIR RDD: aggregate sales by product category.
    val categorySalesRDD = enrichedOrders
      .select("category", "quantity", "order_amount")
      .rdd
      .map { row =>
        (
          row.getAs[String]("category"),
          (
            row.getAs[Int]("quantity").toLong,
            row.getAs[Double]("order_amount")
          )
        )
      }
      .reduceByKey(
        (a: (Long, Double), b: (Long, Double)) =>
          (a._1 + b._1, a._2 + b._2),
        4
      )

    println("\n========== 4. PAIR RDD CATEGORY SALES ==========")

    categorySalesRDD
      .sortByKey()
      .collect()
      .foreach {
        case (category, (units, revenue)) =>
          println(
            f"Category=$category, UnitsSold=$units, Revenue=$revenue%.2f"
          )
      }

    // DATAFRAME WINDOW: cumulative customer spend by order date.
    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("order_date"))
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    val customerSpend = enrichedOrders
      .withColumn(
        "cumulative_spend",
        sum(col("order_amount")).over(customerWindow)
      )

    println("\n========== 5. CUSTOMER SPENDING WINDOW ==========")
    customerSpend
      .select(
        "customer_id",
        "order_id",
        "order_date",
        "order_amount",
        "cumulative_spend"
      )
      .orderBy("customer_id", "order_date", "order_id")
      .show(false)

    // SPARK SQL reporting.
    enrichedOrders.createOrReplaceTempView("enriched_orders")

    println("\n========== 6. CATEGORY REPORT ==========")

    spark.sql(
      """
        |SELECT category,
        |       COUNT(DISTINCT order_id) AS order_count,
        |       SUM(quantity) AS units_sold,
        |       ROUND(SUM(order_amount), 2) AS revenue
        |FROM enriched_orders
        |GROUP BY category
        |ORDER BY revenue DESC
        |""".stripMargin
    ).show(false)

    println("\n========== 7. DAILY SALES REPORT ==========")

    spark.sql(
      """
        |SELECT order_date,
        |       COUNT(DISTINCT order_id) AS order_count,
        |       ROUND(SUM(order_amount), 2) AS daily_revenue
        |FROM enriched_orders
        |GROUP BY order_date
        |ORDER BY order_date
        |""".stripMargin
    ).show(false)

    // Write reports to separate output folders.
    categorySalesRDD
      .map {
        case (category, (units, revenue)) =>
          (category, units, revenue)
      }
      .toDF("category", "units_sold", "revenue")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/category_sales")

    customerSpend
      .select(
        "customer_id",
        "order_id",
        "order_date",
        "order_amount",
        "cumulative_spend"
      )
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/customer_spending")

          spark.sql(
      """
        |SELECT order_date,
        |       COUNT(DISTINCT order_id) AS order_count,
        |       ROUND(SUM(order_amount), 2) AS daily_revenue
        |FROM enriched_orders
        |GROUP BY order_date
        |ORDER BY order_date
        |""".stripMargin
    )

      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/daily_sales")

    println("\nPipeline completed successfully!")
    println("Reports saved under output/")

    enrichedOrders.unpersist()
    cleanOrders.unpersist()
    spark.stop()
  }
}
