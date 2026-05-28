Task1：数据清洗与结构化 —— 实验报告
> 课程：大数据处理综合实验（2026）·课程设计-2：基于 Spark RDD 的搜索引擎检索与排序综合实验
> 子任务：Task1 数据清洗与结构化
> 实现语言：Scala 2.12.10 + Spark 3.0.0 RDD

# 1. 任务目标
输入：原始页面 `raw_pages.tsv`、原始链接 `raw_links.tsv`、停用词表 `stopwords.txt`。
输出：
   `clean_pages.tsv` —— 仅含小写英数 token 的干净正文，供 Task2/Task3 做分类与倒排索引。
   `clean_links.tsv` —— 经过合法性校验、去自环、去重后的页面有向边，供 Task4 做 PageRank。
   `clean_stats.json` —— 清洗前后规模与删除数，便于自查与调试。

# 2. 程序总体流程
                 ┌─────────────────────────────────────────────────┐
 raw_pages.tsv ──┤ 1) flatMap(parsePageLine) → RDD[Array[String]]  │
                 │ 2) persist + count = pagesBefore                │
                 │ 3) map(cleanText) → 拼 TSV → saveAsSingleFile   │──► clean_pages.tsv
                 │ 4) map(_(0)).collect.toSet → broadcast(validIds)│
                 └─────────────────────────────────────────────────┘
                                  │
                                  ▼ broadcast
                 ┌─────────────────────────────────────────────────┐
 raw_links.tsv ──┤ 5) flatMap(parseLinkLine) → persist             │
                 │ 6) filter(端点合法) → filter(非自环)            │
                 │ 7) distinct → saveAsSingleFile                  │──► clean_links.tsv
                 └─────────────────────────────────────────────────┘
                                  │
 stopwords.txt → broadcast        ▼
                 ┌─────────────────────────────────────────────────┐
                 │ 8) 拼接 JSON 字符串 → saveAsSingleFile          │──► clean_stats.json
                 └─────────────────────────────────────────────────┘


整个流水线在 `main` 方法的 try/finally 块内完成，无论中途是否抛异常，最后都会 `sc.stop()` 释放资源。代码主要划分如下：
**入口**：`main(args)` —— 解析命令行参数、构造 SparkContext、串起 8 个步骤。
**解析层**：`parsePageLine` / `parseLinkLine` / `parseWordLine` —— 三个独立的"line → Option[record]"函数，配合 `flatMap` 自动丢弃 `None`。
**清洗核心**：`cleanText(raw, stopwords)` —— 严格按计划书 10 条规则顺序应用预编译正则。
**持久化辅助**：`saveAsSingleFile` 与 `saveWithHeader` —— 用 Hadoop FS API 把 Spark 默认产出的"目录 + part-00000"重命名为单文件，匹配评测脚本对文件名的要求。

# 3. 核心算法与关键实现

3.1 页面文本清洗（10 条规则，严格按序执行）
| # | 规则                   | 实现方式 |
| 1 | 转小写                 | `raw.toLowerCase` |
| 2 | 去 `{{...}}` 模板      | 预编译正则 `(?s)\{\{.*?\}\}` —— `(?s)` 让 `.` 匹配换行，`.*?` 非贪婪保证只匹配最近的 `}}` |
| 3 | `[[A\|B]]` → B        | `\[\[([^\]\|]*)\|([^\]]*)\]\]`，替换为 `Regex.quoteReplacement(m.group(2))` |
| 4 | `[[A]]` → A           | `\[\[([^\]]*)\]\]`，替换为 `Regex.quoteReplacement(m.group(1))` |
| 5a | `<ref ...>...</ref>` | `(?s)<ref[^>]*>.*?</ref>` —— `[^>]*` 自然覆盖三种属性形式 |
| 5b | `<ref name="x" />`   | `<ref[^>]*/>` 自闭合 |
| 6 | 去 HTML 标签           | `<[^>]+>` |
| 7 | 去 URL                | `https?://\S+` 与 `www\.\S+` 两条并列 |
| 8 | 仅留 a-z 0-9 与空格    | `[^a-z0-9 ]` → 空格；`\s+` → 单空格 |
| 9 | 去停用词               | `split(" ").filter(!stopwords.contains(_))` |
| 10 | 去长度 < 2 的 token   | 同上 filter 中 `w.length >= 2` |

**两处关键顺序约束**：
(1)规则 5a 必须先于规则 6：若让规则 6 先去掉所有 `<...>`，则 `<ref>` 仅作为标签本身被删，中间的 URL/正文反而保留下来污染 token。
(2)规则 7 必须先于规则 8：若让规则 8 先把 `://`、`.`、`/` 抹成空格，则 URL 残骸（如 `example com a x 1`）就会被切成多个"普通单词"进入词表。

**正则预编译**：所有 10 条正则以 `private val P_XXX: Regex = "...".r` 形式声明在 `object` 顶层。Scala 的 `"...".r` 等价于 `java.util.regex.Pattern.compile`，是有 CPU 开销的；预编译后全程复用一个 `Regex` 实例，4000 篇文档 × 10 条正则的清洗只编译 10 次（而不是 40000 次）。

**防御性写法**：规则 3/4 用 `Regex.quoteReplacement(...)` 包住替换串。因为 Scala 的 `replaceAllIn(t, replacement)` 会把 replacement 当"替换串"解释，`$1` 是反向引用、`\\` 是转义。若 `raw_text` 里恰好出现 `[[价格|$5]]` 这种情况，不做 `quoteReplacement` 就会抛 `IndexOutOfBoundsException`。

4.2 链接清洗（5 条规则）

```scala
val filtered = rawLinks
  .filter(a => brIds.value.contains(a(0)) && brIds.value.contains(a(1)))  //规则 1+2：两端都在页面集合
  .filter(a => a(0) != a(1))                                              //规则 3：去自环
  .map(a => (a(0), a(1)))                                                  //转元组方便 distinct

val distinctLinks = filtered.distinct()                                    //规则 4：去重
```

规则 5（字段缺失/格式错误）在 `parseLinkLine` 入口层就处理：返回 `None` 后被 `flatMap` 自动丢弃。

`distinct()` 内部是 `reduceByKey`，会触发一次 shuffle —— 是整个 Task1 中**最耗时的算子**。但 4000 页 × 链接平均度数较低，shuffle 数据量小（46364 条），实测耗时可接受。

4.3 清洗统计 JSON

```json
{
  "pages_before": 4000,
  "pages_after": 4000,
  "links_before": 46364,
  "links_after": 45902,
  "removed_illegal_edges": 3,
  "removed_duplicate_edges": 459
}
```

`removed_illegal_edges` = (规则 1+2+3 过滤前) − (过滤后)，即 `linksBefore − afterIllegal`。
`removed_duplicate_edges` = (distinct 前) − (distinct 后)，即 `afterIllegal − linksAfter`。
实验平台结果：45902 + 3 + 459 = 46364

# 4. 优化工作

4.1 广播变量（Broadcast）替代闭包传值

`stopwords` 和 `validIds` 都是小的、被多个 executor 反复查*的数据。如果直接放进闭包里，Spark 会把整个 `Set` 序列化进每个 task —— 4000 页若分成 16 partition 就要传 16 份。改用 `sc.broadcast(...)` 后只传 1 份到每个 executor，executor 内部多个 task 共享同一份 broadcast 副本。

```scala
val brStop:  Broadcast[Set[String]] = sc.broadcast(stopwords)   // ~几百词
val brIds:   Broadcast[Set[String]] = sc.broadcast(validIds)    // 4000 个 doc_id
```

4.2 RDD 复用 + persist
`pages` 在流水线里被使用3 次（count、map+save、collect 提取 doc_id）；`rawLinks` 被使用 2 次（count、filter+distinct）。如果不 persist，每次 action 都会触发完整的 textFile + flatMap 重算，I/O 开销大。

```scala
pages.persist(StorageLevel.MEMORY_AND_DISK)
rawLinks.persist(StorageLevel.MEMORY_AND_DISK)
distinctLinks.persist(StorageLevel.MEMORY_AND_DISK)
```

`MEMORY_AND_DISK` 而非 `MEMORY_ONLY`：当数据量超过内存时自动溢写到磁盘，比 `MEMORY_ONLY` 的"内存不够就丢弃"更稳健。

4.3 Kryo 序列化

```scala
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```
默认 Java 序列化对字符串和小元组性能较差；Kryo 一般快 2-5×，shuffle 阶段尤其明显。Task1 涉及 `distinct()` 的 shuffle，可以受益。

4.4 单文件输出（Hadoop FS API rename）

课程平台评测脚本按文件名打开输出（如 `clean_pages.tsv`），而 Spark 默认的 `saveAsTextFile(path)` 会把 `path` 当目录创建，里面放 `_SUCCESS`、`part-00000`、`.crc` 等。

解决方案：先写到临时目录，再用 Hadoop FS API 把 `part-00000` 重命名为目标文件，同时清掉临时目录：

```scala
private def saveAsSingleFile(sc: SparkContext, rdd: RDD[String], targetPath: String): Unit = {
  val tmpDir = s"${targetPath}__tmp_${System.currentTimeMillis()}"
  rdd.coalesce(1).saveAsTextFile(tmpDir)

  val dstPath = new Path(targetPath)
  val srcPath = new Path(tmpDir, "part-00000")
  val fs      = dstPath.getFileSystem(sc.hadoopConfiguration)

  if (fs.exists(dstPath)) fs.delete(dstPath, true)
  if (!fs.rename(srcPath, dstPath)) {
    throw new RuntimeException(s"saveAsSingleFile rename 失败: $srcPath -> $dstPath")
  }
  fs.delete(new Path(tmpDir), true)
}
```

4.5 表头写入：union + coalesce(1) 无 shuffle 拼接

```scala
val headerRdd = sc.parallelize(Seq(header), numSlices = 1)
saveAsSingleFile(sc, headerRdd.union(body), path)
```

`union` 后整体仍保持父分区的索引顺序：headerRdd 在前（分区 0），body 在后。`coalesce(1, shuffle=false)` 不触发 shuffle，按父分区索引顺序逐个串起所有元素，因此 `part-00000` 的第一行就是表头。

# 5. 自查与正确性验证

按 PDF 第 30 页"自查提示"逐项核对（平台实际运行结果）：

| 自查项 | 期望 | 实际 | 结论 |
| 页面数一般不应明显减少 | pages_after ≈ pages_before | 4000 → 4000 | ✓ |
| 清洗后链接数应略少于清洗前 | links_after < links_before | 46364 → 45902 | ✓ |
| 清洗前后链接数不应几乎相同 | removed > 0 | 删除 462 条（3 非法 + 459 重复） | ✓ |
| 清洗后链接数不应明显过少 | 不大量误删 | ~99% 保留率 | ✓ |
| 数学自洽性 | removed_illegal + removed_dup + links_after == links_before | 3 + 459 + 45902 = 46364 | ✓ |

# 6. 运行结果

平台运行后的 `clean_stats.json`（4000 页全量数据集）：

```json
{
  "pages_before": 4000,
  "pages_after": 4000,
  "links_before": 46364,
  "links_after": 45902,
  "removed_illegal_edges": 3,
  "removed_duplicate_edges": 459
}
```

**结果解读**：
页面：4000 篇全部通过 parsePageLine 校验，无字段缺失/表头/空 doc_id 等问题。
链接：99.0% 保留率，符合精心整理过的课程数据集预期。
非法边仅 3 条：说明 raw_links.tsv 中有 3 条边的端点不在 4000 页集合内（或自环），数据本身整洁度很高。
重复边 459 条（约 1%）：原始数据里偶有重复记录，distinct 后规模轻微下降，符合常理。


# 7. 编译与执行说明

在课程平台运行：

spark-submit \
  --class com.njuse.exp2.Task1Cleaning \
  --master yarn \
  exp2-task1-scala_2.12-1.0.0.jar \
  /user/root/final_exp/exp2/raw_pages.tsv \
  /user/root/final_exp/exp2/raw_links.tsv \
  /user/root/final_exp/exp2/stopwords.txt \
  /user/<用户名>/final_exp/exp2/output/task1

本地运行：

spark-submit \
  --class com.njuse.exp2.Task1Cleaning \
  --master local[*] \
  exp2-task1-scala_2.12-1.0.0.jar \
  ./sample/raw_pages.tsv \
  ./sample/raw_links.tsv \
  ./sample/stopwords.txt \
  ./output/task1 \
  --local


# 8、小结
本任务满足了以下要求：
完整性：10 条文本规则与 5 条链接规则一条都不能漏，且文本规则之间有**严格的顺序约束**（5a 先于 6、7 先于 8）。
范式正确性：核心计算必须留在 RDD 算子里；只有"小查表"数据可以 collect 到 Driver 再 broadcast 回去。
工程细节：单文件输出、表头写入、`Regex.quoteReplacement` 防御、Kryo 序列化、persist 复用等优化对最终性能与评测通过率有直接影响。
经平台评测验证，本任务输出格式、字段完整性、数值合法性均满足 baseline 要求。
