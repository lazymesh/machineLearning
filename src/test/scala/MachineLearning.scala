import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by hadoop on 1/13/18.
  */
class MachineLearning extends FunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach() : Unit = {
    super.afterEach()
    spark.stop
  }

  test("wholetextfiles"){

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

        val rddData = sc.wholeTextFiles("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources")
          .map(x => (x._1, x._2.replace("\n", " "))).foreach(println)
  }

  test("tfidf"){

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

//    val rddData = sc.wholeTextFiles("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources")
//      .flatMap(x => {x._2.split("\n").map(y => (x._1, y))}).foreach(println)
    val sentenceData = spark.createDataFrame(
      sc.wholeTextFiles("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources")
//      .flatMap(x => {x._2.split("\n").map(y => (x._1, y))})
      .map(x => (x._1, x._2.replace("\n", " ")))
    ).toDF("label", "sentence")

//    sentenceData.show(false)

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.printSchema()

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)
    rescaledData.printSchema()
  }

  test("stopwordsRemover"){
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext


    val sentenceData = spark.createDataFrame(
    sc.wholeTextFiles("/home/hadoop/IdeaProjects/AscolSbt/src/test/resources")
    .map(x => (x._1.substring(x._1.lastIndexOf("/")+1, x._1.lastIndexOf(".")), x._2.replace("\n", " ")))
    ).toDF("label", "sentence")

    import org.apache.spark.ml.feature.Tokenizer
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

//    wordsData.select("words").show(false)
    import org.apache.spark.sql.functions._
    val pivoted = remover.transform(wordsData).select("label", "filtered").groupBy("label").pivot("label").agg(first("filtered"))
pivoted.show(false)
//    val exploded = pivoted.select(explode(col(pivoted.columns.drop(1).head)))
//
//    import org.apache.spark.ml.feature.StringIndexer
//    val indexer = new StringIndexer()
//      .setInputCol("col")
//      .setOutputCol("categoryIndex")
//
//    val indexed = indexer.fit(exploded).transform(exploded)
//    val maxi = indexed.select(max(col("categoryIndex"))).first()(0)
//    println(maxi)
//    indexed.filter(col("categoryIndex") === maxi).show(false)
//    indexed.filter(col("col") === "applying").show(false)
  }

  test("svd"){

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext

    import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

    val rows = sc.parallelize(data)

    val mat: RowMatrix = new RowMatrix(rows)

    // Compute the top 5 singular values and corresponding singular vectors.
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s     // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V     // The V factor is a local dense matrix.

    val collect = U.rows.collect()
    println(s"U factor is:\n$U")
    collect.foreach { vector => println(vector) }
    println(s"Singular values are: $s")
    println(s"V factor is:\n$V")
  }

}
