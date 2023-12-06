package com.meetup.sydney.java.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.*;

public class App {

  public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
      .appName("Java Spark SQL basic example")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    Map<String, String> params = new HashMap<>();
    params.put("data_path", "data/good_read_books");
    runJob(spark, params);

    spark.stop();
  }

  public static void runJob(SparkSession spark, Map<String, String> params) {

    System.out.println("Parameters passed:");
    System.out.println(params.toString());

    //WARNING! AWS Glue converts `-`(dash) into `_` (underscore)
    final String DATA_PATH = params.getOrDefault("data_path", "");//default is the current directory

    Dataset<Row> GoodreadsBookAuthorsJsonDf = spark.read().json(DATA_PATH + "/goodreads_book_authors.json");
    Dataset<Row> GoodreadsBookGenresInitialJsonDf = spark.read().json(DATA_PATH + "/goodreads_book_genres_initial.json");
    Dataset<Row> GoodreadsBooksJsonDf = spark.read().json(DATA_PATH + "/goodreads_books.json");

    GoodreadsBookAuthorsJsonDf.
      withColumn("author_id", col("author_id").cast("integer")).
      repartitionByRange(col("author_id")).
      write().parquet(DATA_PATH + "/parquet/goodreads_book_authors.parquet");
    GoodreadsBookGenresInitialJsonDf.
      withColumn("book_id", col("book_id").cast("integer")).
      repartitionByRange(col("book_id")).
      write().parquet(DATA_PATH + "/parquet/goodreads_book_genres_initial.parquet");
    GoodreadsBooksJsonDf.
      withColumn("book_id", col("book_id").cast("integer")).
      repartitionByRange(col("book_id")).
      write().parquet(DATA_PATH + "/parquet/goodreads_books.parquet");

    Dataset<Row> bookAuthorsDs = spark.read().parquet(DATA_PATH + "/parquet/goodreads_book_authors.parquet");
    Dataset<Row> bookGenresDs = spark.read().parquet(DATA_PATH + "/parquet/goodreads_book_genres_initial.parquet");
    Dataset<Row> booksDs = spark.read().parquet(DATA_PATH + "/parquet/goodreads_books.parquet");

    booksDs.
      select("title_without_series").
      groupBy("title_without_series").
      agg(booksDs.col("title_without_series"), count("title_without_series")).
      orderBy(count("title_without_series").desc()).
      show();

    booksDs.
      select("title_without_series").
      groupBy(booksDs.col("title_without_series")).
      agg(booksDs.col("title_without_series"), count(booksDs.col("title_without_series"))).
      orderBy(count(booksDs.col("title_without_series")).desc()).show();

// Count composite field elements
// and filter based on the count
    booksDs.
      select("authors").
      withColumn("AuthorCount", size(booksDs.col("authors"))).
      where(col("AuthorCount").gt(lit(1))).show(false); // <<--- GREATER THAN

    Dataset<Row> booksProcessedDs = booksDs.select(booksDs.col("book_id"), booksDs.col("title"), booksDs.col("authors"), booksDs.col("publication_year"), booksDs.col("publication_month"), booksDs.col("publication_day")).
      withColumn("authors2", explode(booksDs.col("authors"))). // <-- EXPLODE!
      withColumn("author_id", col("authors2.author_id")).
      withColumn("role", col("authors2.role")).
      withColumn("publication_date", to_date(concat(lpad(booksDs.col("publication_year"), 4, "20"),
                                                    lit("-"),
                                                    lpad(booksDs.col("publication_month"), 2, "0"),
                                                    lit("-"),
                                                    lpad(booksDs.col("publication_day"), 2, "0")),
                                             "yyyy-MM-dd"));



    Dataset<Row> booksAuthorRolesDs = booksProcessedDs.join(bookAuthorsDs, booksProcessedDs.col("author_id").
                                                        equalTo(bookAuthorsDs.col("author_id"))).// <<-- EQUAL TO
      select(booksProcessedDs.col("book_id"),
             booksProcessedDs.col("title"),
             booksProcessedDs.col("author_id"),
             bookAuthorsDs.col("name"),
             booksProcessedDs.col("role"),
             booksProcessedDs.col("publication_date"));

    Dataset<Row> genresProcessedDs = bookGenresDs.withColumn("genres_list",
                                                           array_except(array(when(bookGenresDs.col("genres.children").isNotNull(),
                                                                                   lit("children")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.comics, graphic").isNotNull(),
                                                                                   lit("comics:graphic")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.fantasy, paranormal").isNotNull(),
                                                                                   lit("fantasy:paranormal")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.fiction").isNotNull(),
                                                                                   lit("fiction")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.history, historical fiction, biography").isNotNull(),
                                                                                   lit("history:historical_fiction:biography")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.mystery, thriller, crime").isNotNull(),
                                                                                   lit("mystery:thriller:crime")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.non-fiction").isNotNull(),
                                                                                   lit("non-fiction")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.poetry").isNotNull(),
                                                                                   lit("poetry")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.romance").isNotNull(),
                                                                                   lit("romance")).otherwise(null),
                                                                              when(bookGenresDs.col("genres.young-adult").isNotNull(),
                                                                                   lit("young-adult")).otherwise(null)), array(lit(null)))).
      withColumn("genres_string", array_join(col("genres_list"), ","));

    genresProcessedDs.show(false);

    Dataset<Row> booksResultsDs = booksAuthorRolesDs.join(genresProcessedDs, booksAuthorRolesDs.col("book_id")
                                                      .equalTo(genresProcessedDs.col("book_id")))
      .withColumn("author_name", col("name"))
      .drop("genres")
      .drop("name")
      .drop(genresProcessedDs.col("book_id"));

    booksResultsDs.show();

    booksResultsDs.write().parquet(DATA_PATH + "/output/books_result_"+ spark.sparkContext().applicationId() + ".parquet");


    //Using SQL with Spark
    booksResultsDs.createOrReplaceTempView("books");

    Dataset<Row> martinFowlerBooksDs = spark.sql("select * from books where author_name = 'Martin Fowler'");

    System.out.println("Martin Fowler's books:");
    martinFowlerBooksDs.show();

    spark.stop();

  }
}
