### bigdata_final
这是一个基于南京大学大数据课程的期末项目，项目的主要内容是使用Spark进行数据分析和处理  
负责人：李增云，汤思哲  
  
基于Spark RDD的搜索引擎检索与排序综合实验  

```
任务1命令：

```

```
任务2命令：
$SPARK_HOME/bin/spark-submit \
    --class com.example.TextClassification \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    target/spark-scala-project-1.0-SNAPSHOT.jar \
    file:///home/kali/bigdata_final/final-2/clean_pages.tsv \
    file:///home/kali/bigdata_final/final-2/train_ids.txt \
    file:///home/kali/bigdata_final/final-2/test_ids.txt \
    pred_category.tsv classification_report.txt \
    cnb
这里最后输入cnb使用优化的贝叶斯，否则使用朴素贝叶斯
```

```

任务3命令：
spark-submit \
    --class com.example.SearchEngine \
    --conf spark.hadoop.fs.defaultFS=file:/// \
    --master local[*] \
    target/spark-scala-project-1.0-SNAPSHOT.jar \
    file:///home/kali/bigdata_final/final-3/clean_pages.tsv \
    file:///home/kali/bigdata_final/final-3/pred_category.tsv \
    file:///home/kali/bigdata_final/final-3/queries.tsv \
    file:///home/kali/bigdata_final/final-3/inverted_index.tsv \
    file:///home/kali/bigdata_final/final-3/retrieval_topk.tsv \
    5
```

```
任务4命令：

```

```
全流程运行：

```