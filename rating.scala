import org.apache.spark.sql.DataFrame

object rating extends App {

  def createSparkSession(appName: String, Master: String) = {
    org.apache.spark.sql.SparkSession.builder
      .master(Master)
      .appName(appName)
      .getOrCreate
  }
  def readCsvFile(path: String) = {

    val csvSession = createSparkSession("Read CSV", "local")

    csvSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
  }

  def writeCsvFile(dataFrame: DataFrame, name: String) {
    dataFrame
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(name)
  }

  def queryDataFrame(dataFrame: DataFrame,tempView: String, Query: String) = {
    dataFrame.createTempView(tempView)
    val sqlSession = createSparkSession("Query data Frame", "local")
    sqlSession.sql(Query)

  }
    try {
        val df_anime = readCsvFile("src/test/resources/data/anime.csv")

        val df_rating = readCsvFile("src/test/resources/data/rating.csv")

        val df_sum_rating = queryDataFrame(df_rating, "df_rating", "select anime_id , sum(rating) as sum_rated from df_rating group by anime_id")

        val df_joined = df_anime.join(df_sum_rating, usingColumn = "anime_id")
          .filter(df_anime.col("type".toUpperCase) === "TV")
          .filter(df_anime.col("episodes") >= 10)
          .orderBy(org.apache.spark.sql.functions.col("sum_rated").desc)
          .limit(10)

        writeCsvFile(df_joined, "src/test/resources/data/highratedshows.csv")

    } catch {
      case unknown: Exception => 
        println(s"Unknown exception: $unknown")
      
    }
}
