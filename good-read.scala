import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.SaveMode;


/* ====================================================================================================
 * Converting JSON files to parquet format
 * ====================================================================================================
 **/

val DATA_PATH = "data/good_read_books"
val GoodreadsBookAuthorsJsonDf = spark.read.json(s"${DATA_PATH}/goodreads_book_authors.json")
val GoodreadsBookGenresInitialJsonDf = spark.read.json(s"${DATA_PATH}/goodreads_book_genres_initial.json")
val GoodreadsBooksJsonDf = spark.read.json(s"${DATA_PATH}/goodreads_books.json")


GoodreadsBookAuthorsJsonDf.
  withColumn("author_id", $"author_id".cast(IntegerType)).
  repartitionByRange($"author_id").
  write.parquet(s"${DATA_PATH}/parquet/goodreads_book_authors.parquet")
GoodreadsBookGenresInitialJsonDf.
  withColumn("book_id", $"book_id".cast(IntegerType)).
  repartitionByRange($"book_id").
  write.parquet(s"${DATA_PATH}/parquet/goodreads_book_genres_initial.parquet")
GoodreadsBooksJsonDf.
  withColumn("book_id", $"book_id".cast(IntegerType)).
  repartitionByRange($"book_id").
  write.parquet(s"${DATA_PATH}/parquet/goodreads_books.parquet")

//End Converting JSON files to parquet format

val bookAuthorsDf = spark.read.parquet(s"${DATA_PATH}/parquet/goodreads_book_authors.parquet")
val bookGenresDf = spark.read.parquet(s"${DATA_PATH}/parquet/goodreads_book_genres_initial.parquet")
val booksDf = spark.read.parquet(s"${DATA_PATH}/parquet/goodreads_books.parquet")

bookAuthorsDf.printSchema
bookGenresDf.printSchema
booksDf.printSchema

bookAuthorsDf.count //829,529
bookGenresDf.count //2,360,655
booksDf.count //2,360,655


/*
 * GROUP BY Query
 **/
booksDf.
  select("title_without_series").
  groupBy("title_without_series").
  agg($"title_without_series", count($"title_without_series")).
  orderBy(count($"title_without_series").desc).
  show

/*
 * Count Elements of a Composite Field (like a List/Array),
 * and filter the records based on the count
 **/
booksDf.select("authors").
  withColumn("AuthorCount", size($"authors")).
  where($"AuthorCount" > 1)
  .show(false)


/*
 * Reshaping the source data to make it more accessible and friendsly.
 * This includes flatterning composite fields into several primitive fields
 **/
val booksProcessedDf = booksDf.select($"book_id", $"title", $"authors", $"publication_year", $"publication_month", $"publication_day").
  withColumn("authors2", explode($"authors")).    // Flatterning author structure/composite field (part 01)
  withColumn("author_id", $"authors2.author_id"). // Flatterning author structure/composite field (part 02)
  withColumn("role", $"authors2.role").           // Flatterning
  withColumn("publication_date",                  // Constructing a date type field
             to_date(concat(lpad($"publication_year", 4, "20"),
                            lit("-"),
                            lpad($"publication_month", 2, "0"),
                            lit("-"),
                            lpad($"publication_day", 2, "0")),
                     "yyyy-MM-dd"))
/*
 * JOIN Example
 */
val booksAuthorRolesDf = booksProcessedDf.join(bookAuthorsDf,
                                           booksProcessedDf.col("author_id") === bookAuthorsDf.col("author_id")).
  select($"book_id",
         $"title",
         booksProcessedDf.col("author_id"), // prefixing with the original data frame (booksProcessedDf) is required to prevent ambiguity
                                          // as that column present in both tables (otherwise it is an error)
         $"name",
         $"role",
         $"publication_date")

/*
 * Original Genres list is transformed to make list of genre tags (list of strings)
 * and a comma separated list of genre string (single string)
 */
val genresProcessedDf = bookGenresDf.withColumn("genres_list", // list of genre tags (list of strings)
                                              filter(array(
                                                       when($"genres.children".isNotNull, lit("children")).otherwise(null),
                                                       when($"genres.comics, graphic".isNotNull, lit("comics:graphic")).otherwise(null),
                                                       when($"genres.fantasy, paranormal".isNotNull, lit("fantasy:paranormal")).otherwise(null),
                                                       when($"genres.fiction".isNotNull, lit("fiction")).otherwise(null),
                                                       when($"genres.history, historical fiction, biography".isNotNull, lit("history:historical_fiction:biography")).otherwise(null),
                                                       when($"genres.mystery, thriller, crime".isNotNull, lit("mystery:thriller:crime")).otherwise(null),
                                                       when($"genres.non-fiction".isNotNull, lit("non-fiction")).otherwise(null),
                                                       when($"genres.poetry".isNotNull, lit("poetry")).otherwise(null),
                                                       when($"genres.romance".isNotNull, lit("romance")).otherwise(null),
                                                       when($"genres.young-adult".isNotNull, lit("young-adult")).otherwise(null)), (a => a.isNotNull))).
  withColumn("genres_string", array_join($"genres_list", ",")) // comma separated list of genre string (single string)

/*
 * Another Join
 *
 **/
val booksResultsDf = booksAuthorRolesDf.join(genresProcessedDf, booksAuthorRolesDf.col("book_id") === genresProcessedDf.col("book_id")).
  withColumn("author_name", col("name")). // Renaming columns
  drop("name").                           // Renaming columns cont.
  drop("genres").                         // removing unnecesary field
  drop(genresProcessedDf.col("book_id"))


/*
 * Computing our results and writing in parquet format
 **/
booksResultsDf.write.mode(SaveMode.Overwrite).parquet(s"${DATA_PATH}/output/books_result_${spark.sparkContext.applicationId}.parquet")

//Using SQL with Spark
booksResultsDf.createOrReplaceTempView("books");

val martinFowlerBooksDf = spark.sql("select * from books where author_name = 'Martin Fowler'");

println("Martin Fowler's books:");
martinFowlerBooksDf.show;
