package com.njuse.exp2

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Locale
/*
 实验二 Task4 ：PageRank与综合排序
   1) 以 clean_pages.tsv 的全部 doc_id 为节点、clean_links.tsv 的边为有向图，
      手工用 RDD 算子迭代实现带 dangling 修正的 PageRank，得到每个页面的全局
      重要性分值 pagerank。
   2) 读入 Task3 的候选检索结果 retrieval_topk.tsv，对每个 query 的候选文档，
      在"当前 query 内部"分别对 retrieval_score 与 pagerank 做 min-max 归一化，
      按 0.75 : 0.25 融合成 final_score，降序排序后输出最终排序 final_rank.tsv。
 【PageRank 公式】
   PR(v) = (1 - d) / N + d × ( incoming_sum(v) + dangling_mass / N )
     N：页面总数（来自 clean_pages）
     d：阻尼因子，推荐 0.85
     incoming_sum(v)：v 的所有入边贡献之和
     dangling_mass ：所有"无出边节点"的 PR 之和，平均回灌给全部 N 个节点
   推荐参数：iterations = 10, damping = 0.85
 【综合排序公式】
   对每个 query 的候选文档集合，分别对 retrieval_score 和 pagerank 做 min-max：
     norm(x) = (x - min) / (max - min)
     若该 query 下所有候选文档的某分数都相同（max == min），则该项归一化统一设为 1。
   final_score = 0.75 × norm_retrieval_score + 0.25 × norm_pagerank
   按 final_score 降序排序，并列时按 doc_id 升序。

 【输入文件】（Tab分隔）
   clean_pages.tsv      doc_id \t title \t clean_text \t label
   clean_links.tsv      src_id \t dst_id                          
   retrieval_topk.tsv   query_id \t doc_id \t tfidf_score \t category_score
                        \t retrieval_score \t rank          
 【输出文件】（单文件，首行为表头）
   pagerank.tsv    doc_id \t pagerank                            
   final_rank.tsv  query_id \t doc_id \t retrieval_score \t pagerank
                   \t final_score \t rank                         —— 每 query Top-K

 【命令行用法】
   spark-submit --class com.njuse.exp2.Task4PageRank <jar> \
       <cleanPages> <cleanLinks> <retrievalTopk> <outputDir> \
       [iterations=10] [damping=0.85] [--local]
 */
object Task4PageRank {
  /* 每隔多少轮迭代做一次 checkpoint 切断 RDD 血缘。
     为什么需要：PageRank 是迭代算法，第 k 轮的 pr 依赖第 k-1 轮……一路回溯到
     textFile。虽然 persist 会缓存数据，但 RDD 的"血缘 DAG 元数据"仍在不断加长，
     迭代很深时 DAGScheduler 递归提交 stage 可能 StackOverflow。checkpoint 会把
     当前 RDD 物化到可靠存储并截断其父血缘，从根上消除这个隐患。10 轮其实用不到，
     但保留它让代码对"加大 iterations 做优化实验"时依然稳健。 */
  private val CHECKPOINT_INTERVAL: Int = 5

  /* 综合排序的融合权重：final = W_RET × norm_retrieval + W_PR × norm_pagerank。*/
  private val W_RETRIEVAL: Double = 0.75
  private val W_PAGERANK:  Double = 0.25

  /*
    程序入口。标准 Spark 应用骨架：解析参数 → 建 SparkContext → 跑 RDD 链 → stop。
    @param args 命令行参数（顺序）：
                args(0) cleanPagesPath    Task1 输出的 clean_pages.tsv 路径
                args(1) cleanLinksPath    Task1 输出的 clean_links.tsv 路径
                args(2) retrievalTopkPath Task3 输出的 retrieval_topk.tsv 路径
                args(3) outputDir         输出根目录
                args(4) iterations        可选，PageRank 迭代轮数，默认 10
                args(5) damping           可选，阻尼因子，默认 0.85
                args(任意位置) --local    可选，启用本地调试模式
   */
  def main(args: Array[String]): Unit = {
    // 至少要 4 个必填位置参数；不够就打印用法并以非零退出码结束，
    // 让 spark-submit 能据此判定任务失败。
    if (args.length < 4) {
      System.err.println(
        "Usage: Task4PageRank <cleanPages> <cleanLinks> <retrievalTopk> <outputDir> " +
          "[iterations=10] [damping=0.85] [--local]")
      System.exit(1)
    }

    // 解析 4 个必填位置参数。
    val cleanPagesPath:    String = args(0) // clean_pages.tsv
    val cleanLinksPath:    String = args(1) // clean_links.tsv
    val retrievalTopkPath: String = args(2) // retrieval_topk.tsv
    val outputDir:         String = args(3) // 输出根目录

    val extra: Array[String] = args.drop(4).filterNot(_.startsWith("--"))
    val iterations: Int     = if (extra.length > 0) extra(0).toInt    else 10
    val damping:    Double  = if (extra.length > 1) extra(1).toDouble else 0.85
    val localMode:  Boolean = args.contains("--local")

    val sc: SparkContext = buildSparkContext("Exp2-Task4-PageRank", localMode)

    // checkpoint 需要一个可靠目录存放截断后的 RDD 快照。放在 outputDir 下的
    // 隐藏子目录里，跑完后在 finally 里删掉，保持输出目录只剩两个结果文件。
    val checkpointDir: String = s"$outputDir/_checkpoint"
    sc.setCheckpointDir(checkpointDir)

    //try/finally 保证无论是否抛异常，最后都清理 checkpoint 目录并 sc.stop()。
    try {
      val allNodes: RDD[String] = sc.textFile(cleanPagesPath)
        .flatMap(parseCleanPageLine)
        .distinct()
      allNodes.persist(StorageLevel.MEMORY_AND_DISK)

      
      val N: Long = allNodes.count()
      println(s"[Task4] N(nodes from clean_pages) = $N")

      /* 统一的哈希分区器。
         分区数取 max(2, defaultParallelism)，避免本地小数据时分区过少。 */
      val numPartitions: Int = math.max(2, sc.defaultParallelism)
      val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

      val base: RDD[(String, Byte)] = allNodes
        .map(v => (v, 0.toByte))
        .partitionBy(partitioner)
      base.persist(StorageLevel.MEMORY_AND_DISK)

      /*
          从 clean_links.tsv 构建邻接表 adj : (src, 去重后的出边邻居数组)
          flatMap(parseCleanLinkLine) ：每行 → Option[(src, dst)]，丢弃表头/坏行。
          groupByKey(partitioner)     ：按 src 聚合并直接用统一分区器分区。
          mapValues(去重)             ：同一 src 的多条相同 dst 合并（Task1 已去重，
                                        这里再 toSet 一次纯属防御，保证 outdeg 准确）。
          out-degree = 邻居数组长度，恒 ≥ 1（只有出现过边的 src 才会成为 adj 的 key）。
       */
      val adj: RDD[(String, Array[String])] = sc.textFile(cleanLinksPath)
        .flatMap(parseCleanLinkLine)
        .groupByKey(partitioner)
        .mapValues(dsts => dsts.toSet.toArray)
      adj.persist(StorageLevel.MEMORY_AND_DISK)

      /*
          识别 dangling（无出边）节点，收集成 Set 后广播
          dangling 节点 = 在 allNodes 里、但不在 adj.keys 里的节点
          allNodes.subtract(adj.keys) ：集合差。
          为什么广播：每轮迭代都要算 dangling_mass = Σ PR(u)（u 为 dangling），
          做法是对 pr 用一个"是否属于 dangling 集合"的判定来 filter。把这个集合
          广播到每个 executor，filter 闭包里 O(1) 命中，避免每轮再做一次 join/shuffle。
          代价：dangling 集合要 collect 到 driver。4000 节点规模下集合很小，安全。
       */
      val danglingSet: Set[String] = allNodes.subtract(adj.keys).collect().toSet
      val brDangling: Broadcast[Set[String]] = sc.broadcast(danglingSet)
      println(s"[Task4] dangling nodes = ${danglingSet.size}")

      // 后续迭代只用 base 和 adj，这里就可以释放 allNodes 的缓存了。
      allNodes.unpersist()

      /*
        初始化 PR(v) = 1/N
          base.mapValues(_ => 1.0/N) ：mapValues 只改值、不改键，能保留 base 的
          分区器，所以初始 pr 一上来就和 adj 同分区。
       */
      var pr: RDD[(String, Double)] = base.mapValues(_ => 1.0 / N)
      pr.persist(StorageLevel.MEMORY_AND_DISK)

      //迭代计算 PageRank（默认 10 轮）
      for (iter <- 1 to iterations) {
        /* 
           filter 出属于 dangling 集合的节点，取其 PR 值求和。
           用 fold(0.0)(_ + _) 当图中恰好没有 dangling 节点时
           filter 结果为空 带零值的 fold 会安全返回 0.0。 */
        val danglingMass: Double = pr
          .filter { case (id, _) => brDangling.value.contains(id) }
          .map { case (_, rank) => rank }
          .fold(0.0)(_ + _)

        /* 计算入边贡献 incoming_sum：每个有出边的节点把自己的 PR 平均分给
           它的所有出边邻居。
             adj.join(pr) ：把"邻居数组"和"该节点当前 PR"配对（co-partition，窄依赖）
               → (src, (dstArray, prSrc))
             flatMap     ：src 给每个 dst 发送 share = prSrc / outdeg 的贡献
               → 一串 (dst, share)
             reduceByKey(partitioner, _+_) ：同一 dst 收到的所有贡献求和，并用统一
               分区器分区，使 incoming 与 base 同分区，下一步 leftOuterJoin 走窄依赖。 */
        val incoming: RDD[(String, Double)] = adj.join(pr)
          .flatMap { case (_, (dsts, prSrc)) =>
            val share = prSrc / dsts.length
            dsts.iterator.map(dst => (dst, share))
          }
          .reduceByKey(partitioner, _ + _)

        /*套用 PageRank 公式，对"全量节点"更新 PR。
             base.leftOuterJoin(incoming) ：以全量节点为左表，保证没有入边的节点
               （inOpt = None）也参与，不会从结果里消失。
             mapValues ：只改值、保留分区器（用 map 会丢分区器，导致下一轮 join 重 shuffle）。
             inSum = 该节点的入边贡献，没有入边则取 0。
             rank  = (1-d)/N + d × (inSum + danglingMass/N) —— 严格对应公式。 */
        val newPr: RDD[(String, Double)] = base.leftOuterJoin(incoming)
          .mapValues { case (_, inOpt) =>
            val inSum: Double = inOpt.getOrElse(0.0)
            (1.0 - damping) / N + damping * (inSum + danglingMass / N)
          }
        newPr.persist(StorageLevel.MEMORY_AND_DISK)
        if (iter % CHECKPOINT_INTERVAL == 0) newPr.checkpoint()
        val prSum: Double = newPr.map(_._2).fold(0.0)(_ + _)

        // newPr 已物化，上一轮的 pr 不再需要，释放其缓存防止内存堆积。
        pr.unpersist()
        pr = newPr
        // 控制台打印自查：danglingMass 应平稳，prSum 应恒≈1（守恒性验证）。
        println(f"[Task4] iter=$iter%2d  danglingMass=$danglingMass%.6f  PRsum=$prSum%.6f")
      }
      val prLines: RDD[String] = pr.map { case (doc, rank) => s"$doc\t${fmt8(rank)}" }
      saveWithHeader(sc, "doc_id\tpagerank", prLines, s"$outputDir/pagerank.tsv")

      /*
        读取 Task3 的 retrieval_topk.tsv → (doc_id, (query_id, retrieval_score))
          parseRetrievalTopkLine 已经把 retrieval_score（第 5 列，下标 4）解析成
          Double，并做了表头/列数/数值校验。这里把 doc_id 提到 key 上，方便接下来
          与 pagerank（也以 doc_id 为 key）做 join。
       */
      val retrievalByDoc: RDD[(String, (String, Double))] =
        sc.textFile(retrievalTopkPath)
          .flatMap(parseRetrievalTopkLine)
          .map { case (qid, doc, retrieval) => (doc, (qid, retrieval)) }

      /* join pagerank，得到 (query_id, (doc_id, retrieval_score, pagerank))
          retrievalByDoc.join(pr) 以 doc_id 为键内连接，给每条候选记录补上它的
          全局 pagerank。候选文档都来自 clean_pages（Task3 在其上建的索引），
          而 pr 覆盖 clean_pages 全集，所以内连接不会丢候选记录。
          连接后把 query_id 提到 key 上，为下一步"按 query 分组归一化"做准备。
       */
      val candidates: RDD[(String, (String, Double, Double))] =
        retrievalByDoc.join(pr)
          .map { case (doc, ((qid, retrieval), prVal)) =>
            (qid, (doc, retrieval, prVal))
          }

      /*：每个 query 内部 min-max 归一化 → 融合 final_score → 降序 → 编号 rank
          groupByKey() ：把同一 query 的所有候选文档收到一起（每组 ≤ TopK 条，很小）。
          flatMap 内：
            1) 求该 query 内 retrieval / pagerank 的 min、max；
            2) 对每条候选做 min-max 归一化（max==min 时该项规定为 1.0），融合 final_score；
            3) 按 final_score 降序、并列按 doc_id 升序排序（保证结果确定可复现）；
            4) 从 1 开始编号 rank，拼成 6 列 TSV 行。
       */
      val finalLines: RDD[String] = candidates.groupByKey().flatMap { case (qid, it) =>
        //把该 query 的候选物化成数组（每个元素是 (doc, retrieval, pagerank)）。
        val rows: Array[(String, Double, Double)] = it.toArray

        //当前 query 内 retrieval_score 与 pagerank 各自的极值。
        //rows 至少有 1 条（groupByKey 只对有值的 key 出组），min/max 安全。
        val rMin = rows.map(_._2).min; val rMax = rows.map(_._2).max
        val pMin = rows.map(_._3).min; val pMax = rows.map(_._3).max

        //对每条候选：min-max 归一化两路分数，再按权重融合成 final_score。
        //元组结构：(doc_id, 原始 retrieval, 原始 pagerank, final_score)。
        val scored: Array[(String, Double, Double, Double)] = rows.map {
          case (doc, retrieval, prVal) =>
            val nr = if (rMax == rMin) 1.0 else (retrieval - rMin) / (rMax - rMin)
            val np = if (pMax == pMin) 1.0 else (prVal     - pMin) / (pMax - pMin)
            val finalScore = W_RETRIEVAL * nr + W_PAGERANK * np
            (doc, retrieval, prVal, finalScore)
        }

        //排序：final_score 降序（用 -fs 让升序排序等价于降序），并列按 doc_id 升序。
        val sorted = scored.sortBy { case (doc, _, _, fs) => (-fs, doc) }

        //编号并拼行：retrieval 与 final_score 保留 6 位，pagerank 保留 8 位。
        sorted.iterator.zipWithIndex.map { case ((doc, retrieval, prVal, fs), idx) =>
          val rank = idx + 1
          s"$qid\t$doc\t${fmt6(retrieval)}\t${fmt8(prVal)}\t${fmt6(fs)}\t$rank"
        }
      }

      saveWithHeader(sc,
        "query_id\tdoc_id\tretrieval_score\tpagerank\tfinal_score\trank",
        finalLines, s"$outputDir/final_rank.tsv")

      println("[Task4] done.")

      //资源释放
      brDangling.unpersist()
      pr.unpersist()
      adj.unpersist()
      base.unpersist()

    } finally {
      // 删除 checkpoint 临时目录，让 outputDir 下只留 pagerank.tsv / final_rank.tsv。
      // 包一层 try 防止清理失败影响主流程的退出。
      try deletePath(sc, checkpointDir) catch { case _: Throwable => () }
      // 无论上面是否异常，都关闭 SparkContext，释放端口与线程。
      sc.stop()
    }
  }


  private def buildSparkContext(appName: String, localMode: Boolean): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (localMode) conf.setMaster("local[*]")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }

  // 解析 Task1 输出的 clean_pages 一行 → Some(doc_id)，失败返回 None。
    
  private def parseCleanPageLine(line: String): Option[String] = {
    if (line == null || line.isEmpty) return None
    val f = line.split("\t", -1)
    if (f.length != 4) return None
    if (f(0) == "doc_id") return None
    if (f(0).trim.isEmpty) return None
    Some(f(0).trim)
  }

  // 解析 Task1 输出的 clean_links 一行 → Some((src_id, dst_id))，失败返回 None。

  private def parseCleanLinkLine(line: String): Option[(String, String)] = {
    if (line == null || line.isEmpty) return None
    val f = line.split("\t", -1)
    if (f.length != 2) return None
    if (f(0) == "src_id") return None
    if (f(0).trim.isEmpty || f(1).trim.isEmpty) return None
    Some((f(0).trim, f(1).trim))
  }

  /* 解析 Task3 输出的 retrieval_topk 一行 → Some((query_id, doc_id, retrieval_score))。
        query_id \t doc_id \t tfidf_score \t category_score \t retrieval_score \t rank
     Task4 只用到 query_id（第0列）、doc_id（第1列）、retrieval_score（第4列）。
   */
  private def parseRetrievalTopkLine(line: String): Option[(String, String, Double)] = {
    if (line == null || line.isEmpty) return None
    val f = line.split("\t", -1)
    if (f.length != 6) return None
    if (f(0) == "query_id") return None
    val qid = f(0).trim
    val doc = f(1).trim
    if (qid.isEmpty || doc.isEmpty) return None
    try {
      val retrieval = f(4).trim.toDouble // retrieval_score 在第 5 列（下标 4）
      Some((qid, doc, retrieval))
    } catch {
      case _: NumberFormatException => None
    }
  }

  private def fmt6(d: Double): String = String.format(Locale.US, "%.6f", java.lang.Double.valueOf(d))
  private def fmt8(d: Double): String = String.format(Locale.US, "%.8f", java.lang.Double.valueOf(d))

  private def saveWithHeader(sc: SparkContext, header: String,
                             body: RDD[String], path: String): Unit = {
    val withHeader: RDD[String] = body.coalesce(1).mapPartitions { iter =>
      Iterator(header) ++ iter
    }
    saveAsSingleFile(sc, withHeader, path)
  }
  private def saveAsSingleFile(sc: SparkContext, rdd: RDD[String], targetPath: String): Unit = {
    val tmpDir = s"${targetPath}__tmp_${System.currentTimeMillis()}"
    rdd.coalesce(1).saveAsTextFile(tmpDir)

    val dstPath: Path      = new Path(targetPath)
    val srcPath: Path      = new Path(tmpDir, "part-00000")
    val fs:      FileSystem = dstPath.getFileSystem(sc.hadoopConfiguration)

    if (fs.exists(dstPath)) fs.delete(dstPath, true)
    if (!fs.rename(srcPath, dstPath)) {
      throw new RuntimeException(s"saveAsSingleFile rename 失败: $srcPath -> $dstPath")
    }
    fs.delete(new Path(tmpDir), true)
  }

  private def deletePath(sc: SparkContext, path: String): Unit = {
    val p  = new Path(path)
    val fs = p.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(p)) fs.delete(p, true)
  }
}
