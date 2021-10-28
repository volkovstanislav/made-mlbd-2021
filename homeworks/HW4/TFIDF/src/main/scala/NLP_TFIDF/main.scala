package NLP_TFIDF

import org.apache.spark.sql._
import java.util.regex.Pattern
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("tf_idf")
      .getOrCreate()

    import spark.implicits._

    val path_input = "data/tripadvisor_hotel_reviews.csv"
    val path_output = "data/output/"

    var df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(path_input)
    //println(df.show(5))

    /*Привести все к одному регистру*/
    df = df.withColumn("Review", lower(col("Review")))
    //println(df.show(5))

    /*Удалить все спецсимволы*/
    val delSpecialChars = udf((text: String) => {
      val regex = "[\\.\\,\\:\\-\\!\\?\\n\\t,\\%\\#\\*\\|\\=\\(\\)\\\"\\>\\<\\/]"
      val pattern = Pattern.compile(regex)
      val matcher = pattern.matcher(text)
      matcher.replaceAll(" ").split("[ ]+").mkString(" ")
    })

    val clean_df = df.withColumn("Review",
      delSpecialChars(col("Review")))
    //print(clean_df.show(5))

    /*Посчитать частоту слова в предложении*/
    var token_df = clean_df
      .withColumn("Review", split(col("Review")," "))
      .withColumn("token", explode(col("Review")))

    token_df = token_df.withColumn("docId", hash($"Review").cast(LongType)+Int.MaxValue)
    token_df = token_df.drop("Rating")
    token_df = token_df.select(col("docId"), col("token"))
    //print(token_df.show(5))

    val docsWindow = Window
      .partitionBy("docId")

    val tf = token_df
      .groupBy(col("docId"), col("token"))
      .agg(count("token").alias("cnt_token"))
      .withColumn("cnt_tokens_in_doc", sum(col("cnt_token")).over(docsWindow))
      .withColumn("tf", col("cnt_token") / col("cnt_tokens_in_doc"))
      .orderBy(desc("tf"))
    //print(tf.show(5))

    /*Посчитать количество документов со словоми*/
    val cnt_all_docs = tf
      .agg(countDistinct("docId"))

    val idf = tf
      .groupBy(col("token"))
      .agg(countDistinct('docId).alias("cnt_docs_in_token"))
      .withColumn("cnt_all_docs", lit(cnt_all_docs.first().get(0)))
      .withColumn("idf",
        log(col("cnt_all_docs") / col("cnt_docs_in_token")))
      .orderBy(desc("cnt_docs_in_token"))
    //print(idf.show(5))

    /*Взять только 100 самых встречаемых*/
    val top100idf = idf
      .limit(100)
      .select(col("token"), col("idf"))
    //print(top100idf.show())

    /*Сджойнить две полученные таблички и посчитать Tf-Idf (только для слов из предыдущего пункта)*/
    val tfidf = top100idf.as("d1")
        .join(tf.as("d2"), ($"d1.token" === $"d2.token"))
      .select(col("docId"), col("d1.token"), col("tf"), col("idf"))
      .withColumn("tf_idf", col("tf") * col("idf"))
      .orderBy(desc("tf_idf"))

    println(tfidf.show(20))

    /*Сохраняем результаты*/
    tfidf
      .repartition(3)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("header","true")
      .save(path_output)

//    tfidf
//      .write
//      .option("header",true)
//      .format("csv")
//      .save(path_output + "tfidf")
  }
}
