import java.util.Properties

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hadoop on 11/22/17.
  */
object LSI {

  def main(args: Array[String]): Unit = {
    if(args(0).isEmpty){
      println("input is not specified \n default lsi directory will be read")
      System.exit(1)
    }
    val input = args(0)


    val spark = SparkSession.builder().appName("test").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val k = if (!args(1).isEmpty) args(1).toInt else 90
    val numTerms = if (!args(2).isEmpty) args(2).toInt else 20000

    val sentenceData = sqlContext.createDataFrame(
      sc.wholeTextFiles(input)
        .map(x => (x._1.substring(x._1.lastIndexOf("/") + 1), x._2.replace("\n", " ")))
    ).toDF("label", "sentence").as[table]

    val lemmatized = sentenceData.mapPartitions { it => //annotation and annotators are applied

      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP(props)
      it.map { row =>
        val text = row.sentence
        val doc = new Annotation(text)
        pipeline.annotate(doc)
        //
        val lemmas = new ArrayBuffer[String]()
        val sentences: java.util.List[CoreMap] = doc.get(classOf[SentencesAnnotation])

        for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
          val lemma = token.get(classOf[LemmaAnnotation])
          if (lemma.length > 2 && isOnlyLetters(lemma)) {
            lemmas += lemma.toLowerCase
          }
        }
        table(row.label, lemmas.mkString(", "))
      }
    }.select(col("label").as("title"), split(col("sentence"), ", ").as("terms"))

    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
      .setInputCol("terms")
      .setOutputCol("filtered")

    val filtered = remover.transform(lemmatized).select(col("title"), col("filtered").as("terms"))

    val countVectorizer = new CountVectorizer()
      .setInputCol("terms").setOutputCol("termFreqs").setVocabSize(numTerms)
    val vocabModel = countVectorizer.fit(filtered)
    val docTermFreqs = vocabModel.transform(filtered)

    val termIds = vocabModel.vocabulary

    docTermFreqs.cache()

    val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap

    val idf = new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs).select("title", "tfidfVec")
    val termIdfs = idfModel.idf.toArray

    docTermMatrix.cache()

    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row =>
      Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }

    vecRdd.cache()
    val mat = new RowMatrix(vecRdd)
    val svd = mat.computeSVD(k, computeU=true)

    var finalArray = Array.empty[String]
    finalArray = finalArray :+ "total rows and columns " + mat.numRows() +" * " +mat.numCols()
    finalArray = finalArray :+ "used reduced k " + k + "top N terms "+ numTerms + "\n"
    finalArray = finalArray :+ "Singular values: " + svd.s + "\n"
    finalArray = finalArray :+ "right singular : col " + svd.V.numCols + " rows " + svd.V.numRows + "\n"
    finalArray = finalArray :+ "left singular : col " + svd.U.numCols + " rows " + svd.U.numRows + "\n"
    val topConceptTerms = topTermsInTopConcepts(svd, 3, 4, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 3, 4, docIds)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      finalArray = finalArray :+ "Concept terms: " + terms.map(_._1).mkString(", ")
      finalArray = finalArray :+ "Concept docs: " + docs.map(_._1).mkString(", ")
    }

    finalArray = finalArray :+ "\n"

    val queryEngine = new LSAQueryEngine(svd, termIds, docIds, termIdfs)
    finalArray = finalArray :+ "toptermsforterm"
    finalArray = finalArray :+ "crime " + queryEngine.printTopTermsForTerm("crime")
    finalArray = finalArray :+ "murder " + queryEngine.printTopTermsForTerm("murder")
    finalArray = finalArray :+ "kill" + queryEngine.printTopTermsForTerm("kill") + "\n"

    finalArray = finalArray :+ "topDocsFor Term "
    finalArray = finalArray :+ "robbery " + queryEngine.printTopDocsForTerm("robbery")
    finalArray = finalArray :+ "police " + queryEngine.printTopDocsForTerm("police") + "\n"

    finalArray = finalArray :+ "the_way_we_live " + queryEngine.printTopDocsForDoc("the_way_we_live.txt")
    finalArray = finalArray :+ "mob_rule_in_new_orleans " + queryEngine.printTopDocsForDoc("mob_rule_in_new_orleans.txt") + "\n"

    finalArray = finalArray :+ "topDocsForTermQuery : prison, kill, destroy" + queryEngine.printTopDocsForTermQuery(Seq("prison", "kill", "destroy"))

    sc.parallelize(finalArray).coalesce(1).saveAsTextFile("lsi2/output")
    sc.stop()
    spark.stop()

  }
  def isOnlyLetters(str: String) = str.forall(c => Character.isLetter(c))

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, termIds: Array[String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {case (score, id) => (termIds(id), score) }
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                           numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u  = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map { case (score, id) => (docIds(id), score) }
    }
    topDocs
  }

}
class LSAQueryEngine(
                      val svd: SingularValueDecomposition[RowMatrix, Matrix],
                      val termIds: Array[String],
                      val docIds: Map[Long, String],
                      val termIdfs: Array[Double]) {

  val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
  val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
  val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
  val normalizedUS: RowMatrix = distributedRowsNormalized(US)

  val idTerms: Map[String, Int] = termIds.zipWithIndex.toMap
  val idDocs: Map[String, Long] = docIds.map(_.swap)

  /**
    * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
    * Breeze doesn't support efficient diagonal representations, so multiply manually.
    */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

  /**
    * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
    */
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  /**
    * Returns a matrix where each row is divided by its length.
    */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
    * Returns a distributed matrix where each row is divided by its length.
    */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  /**
    * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
    * relevance scores to the given term.
    */
  def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until svd.V.numCols).map(i => svd.V(termId, i)).toArray
    val rowVec = Matrices.dense(rowArr.length, 1, rowArr)

    // Compute scores against every doc
    val docScores = US.multiply(rowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  /**
    * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
    * relevance scores to the given term.
    */
  def topTermsForTerm(termId: Int): Seq[(Double, Int)] = {
    // Look up the row in VS corresponding to the given term ID.
    val rowVec = normalizedVS(termId, ::).t

    // Compute scores against every term
    val termScores = (normalizedVS * rowVec).toArray.zipWithIndex

    // Find the terms with the highest scores
    termScores.sortBy(-_._1).take(10)
  }

  /**
    * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
    * relevance scores to the given doc.
    */
  def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
    // Look up the row in US corresponding to the given doc ID.
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    // Compute scores against every doc
    val docScores = normalizedUS.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._1.isNaN).top(10)
  }

  /**
    * Builds a term query vector from a set of terms.
    */
  def termsToQueryVector(terms: Seq[String]): BSparseVector[Double] = {
    val indices = terms.map(idTerms(_)).toArray
    val values = indices.map(termIdfs(_))
    new BSparseVector[Double](indices, values, idTerms.size)
  }

  /**
    * Finds docs relevant to a term query, represented as a vector with non-zero weights for the
    * terms in the query.
    */
  def topDocsForTermQuery(query: BSparseVector[Double]): Seq[(Double, Long)] = {
    val breezeV = new BDenseMatrix[Double](svd.V.numRows, svd.V.numCols, svd.V.toArray)
    val termRowArr = (breezeV.t * query).toArray

    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def printTopTermsForTerm(term: String): String = {
    val idWeights = topTermsForTerm(idTerms(term))
    idWeights.map { case (score, id) => (termIds(id), score) }.mkString(", ")
  }

  def printTopDocsForDoc(doc: String): String = {
    val idWeights = topDocsForDoc(idDocs(doc))
    idWeights.map { case (score, id) => (docIds(id), score) }.mkString(", ")
  }

  def printTopDocsForTerm(term: String): String = {
    val idWeights = topDocsForTerm(idTerms(term))
    idWeights.map { case (score, id) => (docIds(id), score) }.mkString(", ")
  }

  def printTopDocsForTermQuery(terms: Seq[String]): String = {
    val queryVec = termsToQueryVector(terms)
    val idWeights = topDocsForTermQuery(queryVec)
    idWeights.map { case (score, id) => (docIds(id), score) }.mkString(", ")
  }
}

case class table(label: String, sentence: String)
