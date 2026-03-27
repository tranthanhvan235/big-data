from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("WordCount").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)

lines      = sc.textFile("hdfs://namenode:9000/input/input.txt")
words      = lines.flatMap(lambda line: line.split())
pairs      = words.map(lambda word: (word.lower(), 1))
word_count = pairs.reduceByKey(lambda a, b: a + b)
sorted_wc  = word_count.sortBy(lambda x: x[1], ascending=False)

print("===== WORDCOUNT RESULT =====")
for word, count in sorted_wc.collect():
    print(word + " : " + str(count))
print("============================")
sc.stop()
