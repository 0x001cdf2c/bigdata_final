package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import org.apache.hadoop.fs.{FileSystem, Path}

object TextClassification {

  def main(args: Array[String]): Unit = {
    // 参数: <pages> <train_ids> <test_ids> <pred_out> <report_out> [algo]
    if (args.length < 5) {
      System.err.println("用法: spark-submit ... <clean_pages.tsv> <train_ids.txt> <test_ids.txt> <pred_output> <report_output> [mnb|cnb]")
      System.exit(1)
    }
    val pagesPath    = args(0)
    val trainIdsPath = args(1)
    val testIdsPath  = args(2)
    // 输出路径，支持 HDFS 路径 (如 /user/xxx/out.tsv) 或本地路径 (如 file:///home/xxx/out.tsv)
    val predOutPath   = args(3)
    val reportOutPath = args(4)
    // 算法选择: mnb = Multinomial Naive Bayes, cnb = Complement Naive Bayes
    val algo = if (args.length >= 6) args(5).toLowerCase else "mnb"
    val algoName = algo match {
      case "cnb" => "Complement Naive Bayes"
      case _     => "Multinomial Naive Bayes"
    }

    val spark = SparkSession.builder
      .appName(s"$algoName-TextClassification")
      .getOrCreate()

    val sc = spark.sparkContext

    // 加载清洗后的页面数据 (doc_id \t title \t content \t label)
    val pages = sc.textFile(pagesPath)
      .filter(line => !line.startsWith("doc_id")) // 跳过表头
      .map { line =>
        // \t 分割
        val parts = line.split("\t", 4)
        val docId   = parts(0).trim
        val title   = if (parts.length > 1) parts(1).trim else ""
        val content = if (parts.length > 2) parts(2).trim else ""
        val label   = if (parts.length > 3) parts(3).trim else "UNKNOWN"
        (docId, title, content, label)
      }

    // 加载训练/测试集 ID 列表
    val trainIds: Set[String] = sc.textFile(trainIdsPath)
      .map(_.trim) // 去除首尾空白
      .filter(_.nonEmpty) // 过滤掉空行
      .collect()  // 算子，将 RDD 转换为 Array
      .toSet // 转化为 Set 以便快速查找

    val testIds: Set[String] = sc.textFile(testIdsPath)
      .map(_.trim).filter(_.nonEmpty).collect().toSet

    val trainIdsBc = sc.broadcast(trainIds) // 广播训练集 ID 列表，供后续过滤使用
    val testIdsBc = sc.broadcast(testIds)

    
    // 筛选训练集文档 ,ID 在 train_ids 中 且 label != UNKNOWN
    val trainDocs = pages.filter { case (docId, _, _, label) =>
      trainIdsBc.value.contains(docId) && label != "UNKNOWN"
    }.cache() // cache操作避免后续多次使用时重复计算

    // 筛选测试集文档 (ID 在 test_ids 中)
    val testDocs = pages.filter { case (docId, _, _, _) =>
      testIdsBc.value.contains(docId)
    }.cache()

    
    // 文本分词函数，简单的基于空白和标点的分词，转换为小写，并过滤掉单字母词
    def tokenize(text: String): Array[String] = {
      text.toLowerCase
        .replaceAll("[^a-z]", " ")   // 非字母字符统一替换为空格
        .split("\\s+")               // 按空白字符切分
        .filter(_.length > 1)        // 过滤单字母词
    }

    
    // 训练集分词，注意只对训练集，UNKNOWN 类别不参与训练
    val trainTokenized: RDD[(String, String, Array[String])] = trainDocs.map {
      case (docId, title, content, label) =>
        val words = tokenize(title + " " + content) // 将标题和内容合并后分词
        (docId, label, words)
    }.cache()

    
    // 获取类别列表
    val categories: Array[String] = trainTokenized.map(_._2).distinct().collect()
    val categoriesBc = sc.broadcast(categories)

    /*
      计算类别先验概率 log P(c)
      P(c) = 属于类别 c 的文档数 / 总训练文档数
    */
    val trainDocCount: Long = trainTokenized.count()

    /*
      统计每个类别的文档数
      - map: (docId, label, words) => (label, 1)
      - reduceByKey: 按类别累加计数
      - collectAsMap: 收集到 driver 端，转换为 Map
    */
    val classDocCounts: Map[String, Long] = trainTokenized
      .map(t => (t._2, 1L)) // t._2 是 label，1L 是计数
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap // 转换为 Scala Map 以便后续使用

    // log P(c) 这里使用对数概率，避免后续计算中出现数值下溢问题
    val logPrior: Map[String, Double] = categories.map { c =>
      val cnt = classDocCounts.getOrElse(c, 0L)
      (c, Math.log(cnt.toDouble / trainDocCount))
    }.toMap //现在得到了每个类别的 log P(c)，存储在 logPrior Map 中

    // 将结果广播到各个 worker 节点，供后续计算使用
    val logPriorBc = sc.broadcast(logPrior)

    
    // 构建词表,也就是训练集中所有不重复的词
    val vocabulary: Set[String] = trainTokenized
      .flatMap(_._3)       // flatMap: 展开所有词
      .distinct()           // 去重
      .collect()
      .toSet

    val vocabSize: Int = vocabulary.size

    
    // 统计 count(w, c) —— 词 w 在类别 c 中出现的次数
    // 使用 flatMap 展开 (category, word) 对，然后 reduceByKey 计数
    val wordCountPerClass: Map[(String, String), Long] = trainTokenized
      .flatMap { case (_, label, words) =>
        words.map(w => ((label, w), 1L))
      }
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap
    //  现在得到了一个 Map，键是 (类别, 词)，值是该词在该类别中出现的次数，即 count(w, c)

    // total_words(c) —— 类别 c 中所有词的总出现次数
    val totalWordsPerClass: Map[String, Long] = trainTokenized
      .map { case (_, label, words) => (label, words.length.toLong) }
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap

    // 广播这些映射表 已经得到了 count(w, c) 和 total_words(c)，接下来将它们广播到各个 worker 节点，供后续计算使用
    val wordCountBc   = sc.broadcast(wordCountPerClass)
    val totalWordsBc  = sc.broadcast(totalWordsPerClass)
    val vocabSizeBc   = sc.broadcast(vocabSize)

    // ---- Complement NB 额外统计 ----
    // 每个词在所有类别中的总出现次数: total_count(w) = Σ_c count(w, c)
    val wordTotalCount: Map[String, Long] = wordCountPerClass
      .groupBy(_._1._2)           // 按词分组 (忽略类别)
      .map { case (w, m) => (w, m.values.sum) }
    // 所有类别的总词数: total_words_all = Σ_c total_words(c)
    val totalWordsAll: Long = totalWordsPerClass.values.sum

    val wordTotalCountBc = sc.broadcast(wordTotalCount)
    val totalWordsAllBc  = sc.broadcast(totalWordsAll)

    /*
      对测试集文档进行分类预测
      ── Multinomial NB ──
      score(c, d) = log P(c) + Σ log P(w|c)
      P(w|c) = (count(w,c) + 1) / (total_words(c) + |V|)

      ── Complement NB ──
      score(c, d) = log P(c) - Σ log P(w|¬c)
      P(w|¬c) = (count(w,¬c) + 1) / (total_words(¬c) + |V|)
      count(w,¬c) = total_count(w) - count(w,c)
      total_words(¬c) = total_words_all - total_words(c)
      用减号是因为 P(w|¬c) 越大说明该词越不特有于类别 c，应惩罚
    */
    val predictions: RDD[(String, String)] = testDocs.map {
      case (docId, title, content, _) =>
        val words = tokenize(title + " " + content)

        val (predLabel, _) = algo match {
          // ==================== Multinomial NB ====================
          case "mnb" =>
            categoriesBc.value.map { c =>
              val prior = logPriorBc.value(c)
              val likelihood: Double = words.map { w =>
                val cnt   = wordCountBc.value.getOrElse((c, w), 0L)
                val total = totalWordsBc.value.getOrElse(c, 0L)
                Math.log((cnt + 1.0) / (total + vocabSizeBc.value))
              }.sum
              (c, prior + likelihood)
            }.maxBy(_._2)

          // ==================== Complement NB ====================
          case "cnb" =>
            categoriesBc.value.map { c =>
              val prior = logPriorBc.value(c)
              val totalC = totalWordsBc.value.getOrElse(c, 0L)
              // total_words(¬c) = 所有类别总词数 - 类别c的总词数
              val totalNotC = totalWordsAllBc.value - totalC

              // Σ log P(w|¬c)  —— 补集似然度（惩罚项）
              val complementLikelihood: Double = words.map { w =>
                val cntWC    = wordCountBc.value.getOrElse((c, w), 0L)
                val cntTotal = wordTotalCountBc.value.getOrElse(w, 0L)
                // count(w, ¬c) = 词w在所有类别中的总次数 - 词w在类别c中的次数
                val cntNotC  = cntTotal - cntWC
                Math.log((cntNotC + 1.0) / (totalNotC + vocabSizeBc.value))
              }.sum

              // score = prior - complementLikelihood（减号：词在补集中越常见，惩罚越大）
              (c, prior - complementLikelihood)
            }.maxBy(_._2)

          case _ =>
            throw new IllegalArgumentException(s"未知算法: $algo，可选: mnb | cnb")
        }

        (docId, predLabel)
    }

    
    // 输出文件 1: pred_category.tsv（通过 HDFS API 写出单文件）
    val predHeader = "doc_id\tpred_category"
    val predLines: Array[String] = predHeader +: predictions.collect().map {
      case (id, cat) => s"$id\t$cat"
    }
    val hadoopConf = sc.hadoopConfiguration
    val fs1 = FileSystem.get(new Path(predOutPath).toUri, hadoopConf)
    val pw1 = new PrintWriter(fs1.create(new Path(predOutPath)))
    try {
      predLines.foreach(pw1.println)
    } finally {
      pw1.close()
      fs1.close()
    }

   
    // 输出文件 2: classification_report.txt（通过 HDFS API 写出单文件）
    val testDocCount: Long = testDocs.count()
    val predCategoryCounts: Map[String, Long] = predictions
      .map(_._2)
      .countByValue()
      .map { case (k, v) => (k, v.toLong) }
      .toMap

    val reportLines: Seq[String] = Seq(
      "=" * 50,
      s"   $algoName 文本分类报告",
      "=" * 50,
      "",
      s"1. 训练文档数: $trainDocCount",
      s"2. 测试文档数: $testDocCount",
      s"3. 词表大小:   $vocabSize",
      s"4. 类别列表:   ${categories.mkString(", ")}",
      "",
      "5. 各预测类别数量:",
      predCategoryCounts.toSeq.sortBy(-_._2).map { case (cat, cnt) =>
        s"     $cat: $cnt"
      }.mkString("\n"),
      "",
      "=" * 50
    )

    val fs2 = FileSystem.get(new Path(reportOutPath).toUri, hadoopConf)
    val pw2 = new PrintWriter(fs2.create(new Path(reportOutPath)))
    try {
      reportLines.foreach(pw2.println)
    } finally {
      pw2.close()
      fs2.close()
    }

    trainDocs.unpersist()
    trainTokenized.unpersist()
    testDocs.unpersist()

    spark.stop()
  }
}
