package com.njuse.exp2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
// Hadoop FS API：用来在 saveAsTextFile 写完后，把 part-00000 重命名为目标单文件
// 并清理 _SUCCESS / .crc / 临时目录等辅助物。对 HDFS 和本地 FileSystem 都通用。
import org.apache.hadoop.fs.{FileSystem, Path}
//Scala 风格的正则
import scala.util.matching.Regex


/*
 实验二 Task1 ：数据清洗与结构化
 把"原始 Wiki 页面 + 原始链接 + 停用词"读进来，先把页面里乱七八糟的 wiki
 语法/HTML/URL/标点全部清掉，得到一份只剩英文单词与数字的 clean_text；
 再把链接里那些"两端不存在的/自环的/重复的"统统过滤掉。最后把清洗
 结果与清洗统计写到磁盘，供后续 Task2/3/4 使用。
 【输入文件】
 raw_pages.tsv     doc_id \t title \t raw_text \t label              
 raw_links.tsv     src_id \t dst_id                                  
 stopwords.txt     每行一个停用词
 课程平台 HDFS 数据严格按上述列数；本地 sample 每行多一列"行号"前缀且
 首行是表头。代码内部用 isInteger(f(0)) 自动判别并剥离行号，跳过表头。
 【输出文件】（每个输出都是 Spark 目录，含 _SUCCESS 和 part-00000）
 clean_pages.tsv/    首行表头 + 清洗后的页面，每行 4 列              
 clean_links.tsv/    首行表头 + 清洗后的链接，每行 2 列              
 clean_stats.json/   JSON 文本，记录清洗前后规模与删除数（用于自查）
【命令行用法】
  spark-submit --class com.njuse.exp2.Task1Cleaning  <jar> <rawPages>  <rawLinks>  <stopwords>  <outputDir>  [--local]
 --local ：本地调试，等价于 master = local[*]（用本机所有 CPU 核）
 */
object Task1Cleaning {
  /*
  文本清洗用的 10 条预编译正则
    为什么要"预编译"：把字符串 "..." 调 .r 得到 Regex 对象的过程会触
    发 java.util.regex.Pattern.compile，是有开销的。如果在 cleanText
    里每条记录都现写 "...".r，就会对每行做一次编译——百万级页面就是百万
    次编译。放在 object 顶层一次性编译，全程复用，性能更好。
  */
  /*规则 2：删除 {{...}} 模板。
     匹配从首个 {{ 到 **最近** 的 }}（非贪婪），不处理模板嵌套
    。(?s) 是为了让模板跨多行也能匹配。 */
  private val P_TEMPLATE: Regex = "(?s)\\{\\{.*?\\}\\}".r

  /*规则 5a：删除成对的 <ref ...>...</ref>（参考文献标签）。
   非贪婪，跨行可用。必须先于规则 6（去 HTML 标签）执行，否则规则 6
   会先把 <ref> 当普通标签删掉、只留中间的 URL，结果污染 token。 */
  private val P_REF_PAIR: Regex = "(?s)<ref[^>]*>.*?</ref>".r

  //规则 5b：删除自闭合的 <ref name="x" />。
  private val P_REF_SELF: Regex = "<ref[^>]*/>".r

  /*规则 3：[[A|B]] → 显示文本B。
   组 1 = A（不含 ]和 |），组 2 = B（不含 ]）。
   替换时只保留组 2。计划书声明不处理 A 里含 | 的复杂情况。 */
  private val P_LINK_PIPE: Regex = "\\[\\[([^\\]\\|]*)\\|([^\\]]*)\\]\\]".r

  //规则 4：[[A]] → A。组 1 = A（不含 ]）
  private val P_LINK_PLAIN: Regex = "\\[\\[([^\\]]*)\\]\\]".r

  /*规则 6：删除 HTML 标签 <...> 本身（保留标签内的文本）。
   例如 <p>hello</p>→  hello 。 */
  private val P_HTML: Regex = "<[^>]+>".r

  /*规则 7a：http / https URL。\S+ 表示一串非空白字符。 */
  private val P_URL_HTTP: Regex = "https?://\\S+".r

  /*规则 7b：以www.起头的 URL。
   注意 7a/7b 必须在规则 8（仅留 a-z0-9 与空格）之前执行，
  否则规则 8 会先把 ://、.、/抹成空格，URL 残骸混进 token。 */
  private val P_URL_WWW: Regex = "www\\.\\S+".r

  //规则 8a：除a-z、0-9、空格之外的字符（标点、撇号、引号等）替换为空格。
  private val P_NON_ALNUM: Regex = "[^a-z0-9 ]".r

  //规则 8b：把连续空白合并成单个空格；最后再trim掉首尾空白
  private val P_MULTISPACE: Regex = "\\s+".r

  /*
    程序入口。Spark 应用的标准模式：解析参数 → 建 SparkContext → 跑 RDD 链 → stop。
    @param args 命令行参数（顺序）：
                args(0) rawPagesPath   原始页面文件路径
                args(1) rawLinksPath   原始链接文件路径
                args(2) stopwordsPath  停用词文件路径
                args(3) outputDir      输出根目录（程序会在其下生成 3 个子目录）
                args(后续) --local     可选，启用本地调试模式
   */
  def main(args: Array[String]): Unit = {
    //至少要 4 个必填位置参数；不够就打印用法并以非零退出码结束。
    //用非零退出码（System.exit(1)）能让 spark-submit 知道任务失败。
    if (args.length < 4) {
      System.err.println(
        "Usage: Task1Cleaning <rawPages> <rawLinks> <stopwords> <outputDir> [--local]")
      System.exit(1)
    }
    // 解析命令行参数。
    val rawPagesPath:  String  = args(0)   //原始页面 .tsv 路径
    val rawLinksPath:  String  = args(1)   //原始链接 .tsv 路径
    val stopwordsPath: String  = args(2)   //停用词 .txt 路径
    val outputDir:     String  = args(3)   //输出根目录
    val localMode:     Boolean = args.contains("--local")  // 是否本地模式
    //创建 SparkContext。具体配置见下方 buildSparkContext 方法。
    val sc: SparkContext = buildSparkContext("Exp2-Task1-Cleaning", localMode)
    //用 try/finally 保证无论中途是否抛异常，最后都会 sc.stop() 释放资源
    //（否则进程可能挂起、监听端口不释放）。
    try {
      /*────────────────────────────────────────────────────────────────
       步骤 1 ：加载停用词表，收集到 driver 内存，再广播到所有 executor
         sc.textFile(path) 把文件按行切成 RDD[String]；每行一个元素。
         .flatMap(parseWordLine) 等价于 map+filter+map：parseWordLine
           返回 Option[String]，flatMap 自动剔除 None、把 Some(x) 拍平成 x。
         .collect() 把 RDD 全部内容拉回 driver（变成 Array[String]）。
           仅对小数据可用！这里停用词表通常只有几百个，安全。
          .toSet 转为 Set，O(1) 查找。
      
         stopwords 在 driver 内存里只有一份，但下面 cleanText 在每个 executor
         上都要查它，所以必须广播：sc.broadcast(stopwords) 把这份 Set 高效
         分发到每台 executor 的本地内存，让闭包里能用 brStop.value 访问。*/
      val stopwords:  Set[String]          = sc.textFile(stopwordsPath)
        .flatMap(parseWordLine)
        .collect()
        .toSet
      val brStop:     Broadcast[Set[String]] = sc.broadcast(stopwords)

      println(s"[Task1] stopwords loaded = ${stopwords.size}")
      /*步骤 2 ：解析 raw_pages，得到 RDD[Array[String]]
         每行是 String，parsePageLine 把它按 Tab 切成 Array[String]
         并做格式校验：
            自动剥离 sample 的行号前缀
            跳过表头行（首字段 == "doc_id"）
            跳过空行 / 字段缺失行
         解析失败返回 None；成功返回 Some(Array(doc_id, title, raw_text, label))
         flatMap(parseXxx) 是 Scala 里"map + 过滤无效"的优雅写法。
         pages 在后面被使用 3 次：
           a) 算清洗前页面数 pagesBefore（一次 count）
           b) 清洗写出 clean_pages.tsv（一次 map+save）
           c) 收集 validIds 供链接清洗用（一次 map+collect）
         如果不 persist，pages 会被重新从磁盘读取/解析 3 次；
         .persist(MEMORY_AND_DISK) 把它缓存下来，三次复用同一份。*/
      val pages: RDD[Array[String]] = sc.textFile(rawPagesPath)
        .flatMap(parsePageLine)
      pages.persist(StorageLevel.MEMORY_AND_DISK)

      //触发第一次 action（count）—— 物化 pages，缓存生效
      //同时也得到了清洗前页面数。
      val pagesBefore: Long = pages.count()
      /*步骤 3 ：清洗每条页面的 raw_text，得到 clean_text，写出 clean_pages.tsv
         map { a => ... } ：对每条 Array[String] 调用 cleanText 清洗 raw_text，
         然后用 Scala 字符串插值 s"$a\t$b\t$c\t$d" 拼成 TSV 行。
      
         cleanText 接受的第二个参数是 brStop.value（Set[String]）—— 注意是
         .value，因为我们在 worker 闭包里访问广播变量必须用 .value。
      
         saveWithHeader 是一个我们自己写的辅助方法（见文件底部），
         作用：用 union+coalesce(1) 把"表头单行 + body" 拼成单文件，
               保证 part-00000 的首行就是表头。*/
      val cleanPages: RDD[String] = pages.map { a =>
        val docId   = a(0)
        val title   = a(1)
        val rawText = a(2)
        val label   = a(3)
        val clean   = cleanText(rawText, brStop.value)
        s"$docId\t$title\t$clean\t$label"
      }
      saveWithHeader(sc, "doc_id\ttitle\tclean_text\tlabel",
        cleanPages, s"$outputDir/clean_pages.tsv")

      val pagesAfter: Long = pagesBefore


      /*步骤 4 ：收集合法 doc_id 集合，广播到 executor 供链接清洗用
         pages.map(_(0))  —— 提取每条记录的第 0 列（doc_id）
         .collect().toSet —— 拉回 driver 转成 Set[String]
         sc.broadcast(...) —— 广播
         下面链接清洗的 filter 闭包里就能 brIds.value.contains(...) 做 O(1) 判存。*/
      val validIds: Set[String]            = pages.map(_(0)).collect().toSet
      val brIds:    Broadcast[Set[String]] = sc.broadcast(validIds)


      /*步骤 5 ：解析原始链接 raw_links.tsv
        类似 pages 的解析，每行 Array("src_id", "dst_id")。
        rawLinks 会被 count 一次（统计 linksBefore），后面又被 filter 处理，
        所以也 persist 一下。*/
      val rawLinks: RDD[Array[String]] = sc.textFile(rawLinksPath)
        .flatMap(parseLinkLine)
      rawLinks.persist(StorageLevel.MEMORY_AND_DISK)

      val linksBefore: Long = rawLinks.count()  //清洗前的链接条数


      /*步骤 6 ：链接清洗（5 条规则中的前 3 条 + 第 4 条去重）
       第一个 filter（规则 1+2）：两端都必须在页面集合里
       第二个 filter（规则 3）   ：删除自环 src == dst
       .map(a => (a(0), a(1)))   ：把 Array[String] 转成 (String, String) 元组，方便后面 distinct（distinct 直接作用于元素相等性）*/
      val filtered: RDD[(String, String)] = rawLinks
        .filter(a => brIds.value.contains(a(0)) && brIds.value.contains(a(1)))
        .filter(a => a(0) != a(1))
        .map(a => (a(0), a(1)))

      /*在去重之前 count 一次：这一步的数量 = "在页面集合内 + 非自环" 的边数。
      用它减去 linksBefore 就是被规则 1/2/3 过滤掉的"非法边"数。*/
      val afterIllegal:   Long = filtered.count()
      val removedIllegal: Long = linksBefore - afterIllegal

      //规则 4：去重（distinct 在底层是 reduceByKey，会触发一次 shuffle）。
      //distinctLinks 后面还要 count + map+save，所以 persist。
      val distinctLinks: RDD[(String, String)] = filtered.distinct()
      distinctLinks.persist(StorageLevel.MEMORY_AND_DISK)

      val linksAfter: Long = distinctLinks.count()    //清洗后的链接条数
      val removedDup: Long = afterIllegal - linksAfter //被 distinct 删掉的重复边数


      //步骤 7 ：写出 clean_links.tsv（首行表头）
      //把 (src, dst) 元组用 case 模式匹配解构成两个变量，再拼成 "src\tdst"。
      val cleanLinks: RDD[String] = distinctLinks.map { case (s, d) => s"$s\t$d" }
      saveWithHeader(sc, "src_id\tdst_id",
        cleanLinks, s"$outputDir/clean_links.tsv")


      /*步骤 8 ：写出 clean_stats.json（自查用）
         不引入额外 JSON 库，自己用三引号字符串 + stripMargin 拼一个 JSON 文本。
         stripMargin 会去掉每行 `|` 之前的空白，让源码缩进与输出对齐。
      
         这里走 saveAsSingleFile 把它产成真单文件
         与 clean_pages.tsv / clean_links.tsv
         统一对齐课程平台对评测目录"文件名规范"的要求。*/
      val json: String =
        s"""{
           |  "pages_before": $pagesBefore,
           |  "pages_after": $pagesAfter,
           |  "links_before": $linksBefore,
           |  "links_after": $linksAfter,
           |  "removed_illegal_edges": $removedIllegal,
           |  "removed_duplicate_edges": $removedDup
           |}""".stripMargin

      saveAsSingleFile(
        sc,
        sc.parallelize(json.split("\n").toSeq, numSlices = 1),
        s"$outputDir/clean_stats.json"
      )


      //在控制台打印一份汇总，运行时肉眼自查。
      println(s"[Task1] pages: $pagesBefore -> $pagesAfter")
      println(s"[Task1] links: $linksBefore -> $linksAfter " +
        s"(illegal=$removedIllegal, duplicate=$removedDup)")


      //资源释放：把缓存与广播变量从内存中清除
      //非阻塞模式即可，等 GC 处理；不释放也没关系（程序马上要退出）。
      brStop.unpersist()
      brIds.unpersist()
      distinctLinks.unpersist()
      rawLinks.unpersist()
      pages.unpersist()

    } finally {
      //无论上面是否异常，都关闭 SparkContext。
      sc.stop()
    }
  }

  //以下是本任务工具方法
  /*构造一个统一配置的 SparkContext。
   设置项说明：
     - setAppName(name) ：在 Spark Web UI 上显示的应用名
     - setMaster("local[*]") ：local 模式，[*] 表示用本机全部 CPU 核
            （集群提交时不要 setMaster，交给 spark-submit --master 控制）
     - "spark.ui.showConsoleProgress" = "false"
         关闭命令行的滚动进度条噪声（调试时方便看 println）
      - "spark.serializer" = "...KryoSerializer"
       用 Kryo 取代默认 Java 序列化；字符串/小元组性能更好
   @param appName   应用名（控制台 / Web UI 上看到的标识）
   @param localMode true → 本地单机调试；false → 期望由 spark-submit 提供 master
   @return 已配置好、可用于创建 RDD 的 SparkContext
   */
  private def buildSparkContext(appName: String, localMode: Boolean): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (localMode) conf.setMaster("local[*]")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }


  /**
   判断字符串是否是纯整数（允许前导负号）。
  为什么需要它：本地 sample 的每行多了一列"行号"前缀，例如：
       `2\t10744\tEric Clapton\t"..."\tArts`
   而平台数据没有这一列：
       `10744\tEric Clapton\t"..."\tArts`
    两种格式只差一列。我们在 parsePageLine / parseLinkLine 里通过"列数是不是比标准多 1 且首列是纯整数？" 来识别行号前缀。
   实现细节：
      - 用 while 循环逐字符判断，比 try/catch Integer.parseInt 快
      - 接受前导负号（s.charAt(0) == '-' 且长度 > 1）
   @param s 待判断字符串
    @return true 表示是纯整数（如 "0"、"42"、"-7"）；false 否则
   */
  private def isInteger(s: String): Boolean = {
    if (s == null || s.isEmpty) return false
    //起始位置：若首字符是 '-' 且字符串长度 > 1，则从第二个字符开始扫
    val start = if (s.charAt(0) == '-' && s.length > 1) 1 else 0
    var i = start
    while (i < s.length) {
      val c = s.charAt(i)
      if (c < '0' || c > '9') return false   //一旦遇到非数字字符，立刻判 false
      i += 1
    }
    true
  }


  /*页面文本清洗 —— 按计划书的 10 条规则严格、顺序执行。
   【顺序为什么重要】
     - URL（规则 7）必须先于"只留 a-z0-9"（规则 8）：否则规则 8 会先抹掉
      ://  .  /，导致 URL 残骸（如 example com a x 1）混入 token。
    - 成对 ref（规则 5a）必须先于 HTML（规则 6）：否则规则 6 会先把
        <ref> 当普通标签删掉，只剩中间的 URL/文本，污染 token。
    【Scala 正则替换的小陷阱】
      `Regex.replaceAllIn(t, replacement)` 把 replacement 当作"替换串"解释，
      `$1`、`\\` 在替换串里有特殊含义（反向引用 / 转义）。对纯空格 " " 没问题；
      但对规则 3/4（用 `m.group(2)` / `m.group(1)` 作替换）必须用
      `Regex.quoteReplacement(...)` 包一层，把 `$` 和 `\` 转义成字面量，
      否则万一 raw_text 里出现这些字符就会抛 IndexOutOfBoundsException 或乱替换。
    【函数功能总结】
      输入：原始 raw_text（可能很长、可能含换行、可能含 wiki/HTML 语法）
      输出：以单空格连接的小写英数 token 序列，已去停用词与短词
    @param raw       原始 raw_text；允许 null（按空串处理）
    @param stopwords 停用词集合（O(1) 查找）
    @return 清洗后的 clean_text（可能为空串，但绝不为 null）
   */
  private def cleanText(raw: String, stopwords: Set[String]): String = {
    if (raw == null) return ""

    //一步步把t替换；每一步赋值都是产生新字符串。
    var t = raw.toLowerCase                                          //规则 1 ：转小写

    t = P_TEMPLATE.replaceAllIn(t, " ")                              //规则 2 ：去 {{...}}
    t = P_REF_PAIR.replaceAllIn(t, " ")                              //规则 5a：去 <ref>...</ref>
    t = P_REF_SELF.replaceAllIn(t, " ")                              //规则 5b：去 <ref .../>

    //规则 3 ：[[A|B]] → B。用 quoteReplacement 防御 B 含 `$` 或 `\`
    t = P_LINK_PIPE.replaceAllIn(t, m => Regex.quoteReplacement(m.group(2)))
    //规则 4 ：[[A]] → A
    t = P_LINK_PLAIN.replaceAllIn(t, m => Regex.quoteReplacement(m.group(1)))

    t = P_HTML.replaceAllIn(t, " ")                                  //规则 6 ：去 HTML 标签
    t = P_URL_HTTP.replaceAllIn(t, " ")                              //规则 7a：去 http(s) URL
    t = P_URL_WWW.replaceAllIn(t, " ")                               //规则 7b：去 www URL
    t = P_NON_ALNUM.replaceAllIn(t, " ")                             //规则 8a：仅留 a-z0-9 与空格
    t = P_MULTISPACE.replaceAllIn(t, " ").trim                       //规则 8b：合并连续空白

    //规则 9（去停用词）+ 规则 10（去长度<2 token）一并完成：
    //- split(" ") 按单空格切（前面 8b 已把连续空白合一）
    //- filter 保留同时满足 `长度 ≥ 2` 与 `不在 stopwords 里` 的 token
    //- mkString(" ") 用单空格重新拼回字符串
    t.split(" ")
      .filter(w => w.length >= 2 && !stopwords.contains(w))
      .mkString(" ")
  }


  /*解析 raw_pages 一行 → Some(Array(doc_id, title, raw_text, label))，失败返回 None。
    【支持的两种输入格式】
      平台 HDFS （标准 4 列）：
        `doc_id \t title \t raw_text \t label`
      本地 sample（带行号 5 列）：
        `行号 \t doc_id \t title \t raw_text \t label`
   【判别方法】
      先按 Tab 切；如果切出 5 段且首段是纯整数 → 判定为 sample 行号格式，剥离首段。
      注意 `split("\t", -1)`：limit = -1 保留尾部空字段，防止 `\t\t` 这种连续 tab
      导致字段数少算（默认 split 会丢掉尾部空串）。
   【过滤规则】
      - 空行 → None
      - 列数不是 4（剥离行号后）→ None
      - 首字段是字面量 "doc_id" → 表头行 → None
      - 首字段是空白 → None
   @param line 一行原始文本
   @return Some(4 字段数组) 表示成功；None 表示这行应被丢弃
   */
  private def parsePageLine(line: String): Option[Array[String]] = {
    if (line == null || line.isEmpty) return None
    var f: Array[String] = line.split("\t", -1)

    // 5 列且首列是纯整数 → 是 sample 的"行号"前缀，剥掉
    if (f.length == 5 && isInteger(f(0))) f = Array(f(1), f(2), f(3), f(4))

    if (f.length != 4) return None      //列数不对
    if (f(0) == "doc_id") return None   //表头
    if (f(0).trim.isEmpty) return None  //doc_id 为空白
    Some(f)
  }
  /*解析 raw_links 一行 → Some(Array(src_id, dst_id))，失败返回 None。
    【支持的两种输入格式】
      平台 HDFS：`src_id \t dst_id`   （2 列）
      本地 sample：`行号 \t src_id \t dst_id`  （3 列）
   【过滤规则】
    - 空行、列数不对、表头、src/dst 任一为空 → None
   
   @param line 一行原始文本
   @return Some((src, dst)) 表示成功；None 表示这行应被丢弃
   */
  private def parseLinkLine(line: String): Option[Array[String]] = {
    if (line == null || line.isEmpty) return None
    var f: Array[String] = line.split("\t", -1)

    if (f.length == 3 && isInteger(f(0))) f = Array(f(1), f(2))  // 剥离行号

    if (f.length != 2) return None
    if (f(0) == "src_id") return None
    if (f(0).trim.isEmpty || f(1).trim.isEmpty) return None
    Some(Array(f(0).trim, f(1).trim))
  }


  /* 解析 stopwords 一行 → Some(word)，失败返回 None。
    【trim + toLowerCase】
    cleanText 输出的 token 是小写、不含空白的；停用词必须和它"格式对齐"
    才能成功命中匹配。所以在加载阶段就统一规范化：
     - trim 去首尾空白
     - toLowerCase 转小写
    @param line 一行原始文本（可能是 sample 的 "行号\tword" 也可能是平台的 "word"）
    @return Some(规范化后的小写词) 或 None
   */
  private def parseWordLine(line: String): Option[String] = {
    if (line == null || line.isEmpty) return None
    val f: Array[String] = line.split("\t", -1)

    //兼容行号前缀：2 列且首列整数 → 取第二列；否则取第一列
    val w: String = (if (f.length == 2 && isInteger(f(0))) f(1) else f(0))
      .trim
      .toLowerCase

    if (w.isEmpty || w == "word") None else Some(w)
  }


  /* 把 body RDD 写成"首行带表头"的单文件。
   【历史 bug 与修复说明】
     旧实现：headerRdd.union(body) → saveAsSingleFile（内部 coalesce(1)）
     问题  ：当 body 来源于 textFile 时通常有 N 个分区，union 后变成 N+1 个分区。
             coalesce(1, shuffle=false) 在多分区→单分区合并时不保证按分区
             索引顺序输出：实测在 4000 页规模下表头被写到了文件中间
             （clean_links.tsv 第 23129 行、clean_pages.tsv 第 1989 行），
             评测脚本会把表头当数据行处理，导致 doc_id 数虚增、出现"非法端点"
             等连锁错误。
     修复：先把 body coalesce(1) 收成 1 个分区，再用 mapPartitions 在
             这唯一分区的迭代器最前面插入表头。此时整条 RDD 只有 1 个分区，
             输出顺序完全由 mapPartitions 决定，表头永远在文件首行。
   【实现细节】
     - body.coalesce(1) ：把 N 分区收成 1 分区（不触发 shuffle）
     - mapPartitions(iter => Iterator(header) ++ iter) ：
         在唯一分区的元素迭代器最前面拼接表头；++ 是 Iterator 的惰性拼接，
         writer 读取时先吐出 header，再依次吐出 body 元素，不占额外内存
     - 交给 saveAsSingleFile 完成"临时目录 → 单文件重命名"

    @param sc     SparkContext
    @param header 表头字符串（已经用 \t 拼好）
    @param body   清洗后的内容 RDD（不含表头）
    @param path   目标文件路径（写完后这就是一个真单文件，HDFS 或本地皆可）
   */
  private def saveWithHeader(sc:     SparkContext,
                             header: String,
                             body:   RDD[String],
                             path:   String): Unit = {
    val withHeader: RDD[String] = body.coalesce(1).mapPartitions { iter =>
      Iterator(header) ++ iter
    }
    saveAsSingleFile(sc, withHeader, path)
  }


  /* 把任意 RDD[String] 写成一个单文件。
    【实现】
      1. 用 saveAsTextFile 写到一个临时目录 ${targetPath}__tmp_<时间戳>
         （后缀保证临时目录不会和已有输出冲突）
      2. 用 Hadoop FS API 把 tmpDir/part-00000 重命名为 targetPath
         （Hadoop FS API 对 HDFS 和本地 FileSystem 都通用）
      3. 递归删除临时目录，连带 _SUCCESS、.crc 一并清理
      4. 若 targetPath 已存在（之前可能是上次跑出来的目录或文件），先 delete 再 rename
    【代价】
      coalesce(1) 把所有数据汇聚到 1 个 task 写出，吞吐量较低，但课程数据规模
      小（一份 raw_pages.tsv 才几十 MB），完全可接受。
    @param sc         SparkContext
    @param rdd        要写出的 RDD（可以是任意多个分区，函数内会 coalesce(1)）
    @param targetPath 目标文件路径（写完后这就是一个文件，HDFS 或本地皆可）
   */
  private def saveAsSingleFile(sc: SparkContext, rdd: RDD[String], targetPath: String): Unit = {
    //临时目录用时间戳防冲突（同一进程内若多次写同一 targetPath 也安全）
    val tmpDir = s"${targetPath}__tmp_${System.currentTimeMillis()}"

    //先以 Spark 标准方式写到临时目录；coalesce(1) 保证只有一个 part 文件
    rdd.coalesce(1).saveAsTextFile(tmpDir)

    //用 Hadoop FS API 处理重命名 + 清理
    //从目标路径推断 FileSystem：相对路径 → LocalFileSystem；hdfs:// → DistributedFileSystem
    val dstPath: Path       = new Path(targetPath)
    val srcPath: Path       = new Path(tmpDir, "part-00000")
    val fs:      FileSystem = dstPath.getFileSystem(sc.hadoopConfiguration)

    //目标位置如已存在（上次跑剩的目录或文件），整体删掉（recursive=true 同时支持文件与目录）
    if (fs.exists(dstPath)) fs.delete(dstPath, true)

    //把 part-00000 重命名为目标文件
    if (!fs.rename(srcPath, dstPath)) {
      throw new RuntimeException(s"saveAsSingleFile rename 失败: $srcPath -> $dstPath")
    }

    //删除临时目录（_SUCCESS、.crc 等附属文件也一起清掉）
    fs.delete(new Path(tmpDir), true)
  }
}
