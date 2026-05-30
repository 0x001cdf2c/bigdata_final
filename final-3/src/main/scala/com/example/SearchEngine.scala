package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.io.PrintWriter

/**
 * 基于 Spark RDD 的倒排索引构建与关键词搜索引擎
 *
 * TF(term, doc)       = count(term, doc)
 * DF(term)            = count(doc containing term)
 * IDF(term)           = log((N+1) / (DF(term)+1)) + 1
 * TF-IDF(term, doc)   = TF(term, doc) * IDF(term)
 *
 * retrieval_score     = tfidf_score + 0.2 * category_score
 * category_score      = 1 if doc_category == target_category else 0
 */
object SearchEngine {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        "用法: spark-submit ... <clean_pages.tsv> <pred_category.tsv> <queries.tsv> " +
        "<inverted_index.tsv> <retrieval_topk.tsv> <K>")
      System.exit(1)
    }
    val pagesPath     = args(0)
    val predCatPath   = args(1)
    val queriesPath   = args(2)
    // 去掉可能的 file:// 或 file: 前缀，PrintWriter 需要纯本地路径 用于本地测试
    val invIdxOutPath = args(3).replaceFirst("^file:/*", "/")
    val topKOutPath   = args(4).replaceFirst("^file:/*", "/")
    val K             = args(5).toInt

    val spark = SparkSession.builder
      .appName("InvertedIndex-SearchEngine")
      .getOrCreate()
    val sc = spark.sparkContext

    // 依旧是加载数据，但这次我们需要同时加载清洗后的页面数据和预测的类别数据
    val pages: RDD[(String, String, String, String)] = sc.textFile(pagesPath)
      .filter(line => !line.startsWith("doc_id"))
      .map { line =>
        val parts = line.split("\t", 4)
        val docId   = parts(0).trim
        val title   = if (parts.length > 1) parts(1).trim else ""
        val content = if (parts.length > 2) parts(2).trim else ""
        val label   = if (parts.length > 3) parts(3).trim else "UNKNOWN"
        (docId, title, content, label)
      }
    // 先把测试id用unknown占位，后续再用pred_category替换

    // pred_category: (docId, predCategory)
    val predCat: RDD[(String, String)] = sc.textFile(predCatPath)
      .filter(line => !line.startsWith("doc_id"))
      .map { line =>
        val parts = line.split("\t")
        (parts(0).trim, parts(1).trim)
      }

    // 训练集保留原 label, 测试集(UNKNOWN)用 pred_category
    val predCatMap = predCat.collectAsMap().toMap
    val predCatBc = sc.broadcast(predCatMap)

    // (docId, title, content, finalLabel)
    val allDocs: RDD[(String, String, String, String)] = pages.map {
      case (docId, title, content, label) =>
        val finalLabel =
          if (label == "UNKNOWN") predCatBc.value.getOrElse(docId, "UNKNOWN")
          else label
        (docId, title, content, finalLabel)
    }.cache()

    val N: Long = allDocs.count()

    // 现在得每个文档都有一个最终的类别标签

    
    // 分词: 小写→去非字母数字→按空格切分→过滤长度<2的单词
    def tokenize(text: String): Array[String] = {
      text.toLowerCase
        .replaceAll("[^a-z0-9]", " ")
        .split("\\s+")
        .filter(_.length > 1)
    } 

    // (docId, Array[term]) 现在得到了每个文档的分词结果
    val docTokens: RDD[(String, Array[String])] = allDocs.map {
      case (docId, title, content, _) =>
        (docId, tokenize(title + " " + content))
    }.cache()//以string为key，array[string]为value的RDD，后续计算TF、DF、IDF都基于这个RDD进行

    // 计算TF，也就是每个文档中每个词出现的次数，结果是 ((docId, term), tfVal) 的形式
    val tf: RDD[((String, String), Int)] = docTokens
      .flatMap { case (docId, words) =>
        words.map(w => ((docId, w), 1))
      }
      .reduceByKey(_ + _)

    // 计算DF，也就是每个词出现的文档数量，结果是 (term, dfCount) 的形式
    val df: RDD[(String, Int)] = docTokens
      .flatMap { case (docId, words) =>
        words.distinct.map(w => (w, 1))
      }
      .reduceByKey(_ + _)

    // IDF,类似DF的倒数，更符合语义，结果是 (term, idfVal) 的形式
    val idf: RDD[(String, Double)] = df.map { case (term, dfCount) =>
      (term, Math.log((N + 1.0) / (dfCount + 1.0)) + 1.0)
    }

    // 计算score，也就是TF-IDF，结果是 (docId, term, tfidfVal) 的形式
    val tfIdf: RDD[(String, String, Double)] = tf
      .map { case ((docId, term), tfVal) => (term, (docId, tfVal)) }
      .join(idf)
      .map { case (term, ((docId, tfVal), idfVal)) =>
        (docId, term, tfVal.toDouble * idfVal)
      }

    // 继续把类别信息 join 进来，得到 (term, docId, tfidfVal, category) 的形式，供后续构建倒排索引和搜索使用
    val docCat: RDD[(String, String)] = allDocs.map {
      case (docId, _, _, label) => (docId, label)
    }

    // (docId, (term, tfidfVal)) join (docId, category) → (term, docId, tfidf, category)
    val rawPostings: RDD[(String, String, Double, String)] = tfIdf
      .map { case (docId, term, tfidfVal) => (docId, (term, tfidfVal)) }
      .join(docCat)
      .map { case (docId, ((term, tfidfVal), category)) =>
        (term, docId, tfidfVal, category)
      }.cache()

    // 得到倒排索引的文本格式: term \t docId1:tfidf1:cat1;docId2:tfidf2:cat2;...
    val invIdxHeader = "term\tposting_list"
    val invIdxLines: Array[String] = invIdxHeader +: rawPostings
      .map { case (term, docId, tfidfVal, cat) =>
        (term, s"$docId:${tfidfVal.formatted("%.4f")}:$cat")
      }
      .groupByKey()
      .mapValues(_.mkString(";"))
      .map { case (term, postings) => s"$term\t$postings" }
      .collect()

    val pw1 = new PrintWriter(invIdxOutPath)
    try {
      invIdxLines.foreach(pw1.println)
    } finally {
      pw1.close()
    }

    
    // 构建查询用的 Map[term, Array[(docId, tfidf, category)]] 并广播到各个 worker 节点，供后续搜索使用
    val invIdxMap = rawPostings
      .map { case (term, docId, tfidfVal, cat) => (term, (docId, tfidfVal, cat)) }
      .groupByKey()
      .mapValues(_.toArray)
      .collectAsMap()
      .toMap

    val invIdxBc = sc.broadcast(invIdxMap)

    // 加载查询文件，格式是 query_id \t query_text \t target_category
    val queries: RDD[(String, String, String)] = sc.textFile(queriesPath)
      .filter(line => !line.startsWith("query_id"))
      .map { line =>
        val parts = line.split("\t", 3)
        val qId   = parts(0).trim
        val qText = if (parts.length > 1) parts(1).trim else ""
        val tCat  = if (parts.length > 2) parts(2).trim else ""
        (qId, qText, tCat)
      }

    // 对于每一行查询，进行检索并计算得分，最终得到 
    // (queryId, docId, tfidf_score, category_score, retrieval_score, rank) 的结果
    // 查询id,  文档id, tfidf得分,  类别得分, 综合得分, 排名
    /*
      为什么加入类别得分？
      通过类别约束，使得检索结果更符合用户的意图，提升相关性和用户满意度。
      经典的比如水果“苹果”和手机“苹果”，是不同类别
    */
    val results: RDD[(String, String, String, String, String, String)] = queries.flatMap {
      case (queryId, queryText, targetCat) =>
        val queryTerms = tokenize(queryText)

        if (queryTerms.isEmpty) {
          Seq.empty
        } else {
          // docId → 累计 tfidf
          val docScores = scala.collection.mutable.Map[String, Double]()
          // docId → category
          val docCatMap = scala.collection.mutable.Map[String, String]()

          for (term <- queryTerms) {
            invIdxBc.value.get(term).foreach { postings =>
              for ((docId, tfidfVal, cat) <- postings) {
                docScores(docId) = docScores.getOrElse(docId, 0.0) + tfidfVal
                docCatMap(docId) = cat
              }
            }
          }

          if (docScores.isEmpty) {
            Seq.empty
          } else {
            // 计算 category_score 和 retrieval_score，排序取 TopK
            docScores.map { case (docId, tfidfSum) =>
              val catScore = if (docCatMap.getOrElse(docId, "").contains(targetCat)) 1 else 0
              val retScore = tfidfSum + 0.2 * catScore
              (docId, tfidfSum, catScore, retScore)
            }.toSeq
              .sortBy(-_._4)        // 按 retrieval_score 降序
              .take(K)
              .zipWithIndex
              .map { case ((docId, tfidfSum, catScore, retScore), idx) =>
                (queryId,
                 docId,
                 tfidfSum.formatted("%.4f"),
                 catScore.toString,
                 retScore.formatted("%.4f"),
                 (idx + 1).toString)
              }
          }
        }
    }

    // 保存结果（单个文件）
    val topKHeader = "query_id\tdoc_id\ttfidf_score\tcategory_score\tretrieval_score\trank"
    val topKLines: Array[String] = topKHeader +: results.collect().map {
      case (qid, did, tf, cs, rs, rk) =>
        s"$qid\t$did\t$tf\t$cs\t$rs\t$rk"
    }

    val pw2 = new PrintWriter(topKOutPath)
    try {
      topKLines.foreach(pw2.println)
    } finally {
      pw2.close()
    }

    allDocs.unpersist()
    docTokens.unpersist()
    rawPostings.unpersist()

    spark.stop()
  }
}
