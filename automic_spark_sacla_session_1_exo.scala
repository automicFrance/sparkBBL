// Databricks notebook source
// MAGIC %md
// MAGIC #![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png) + ![Automic Logo](http://files.automic.com/sites/all/themes/automic/logo.png)
// MAGIC # **Word Count Lab: Building a word count application**
// MAGIC 
// MAGIC This lab will build on the techniques covered in the Spark tutorial to develop a simple word count application.  The volume of unstructured text in existence is growing dramatically, and Spark is an excellent tool for analyzing this type of data.  In this lab, we will write code that calculates the most common words in the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) retrieved from [Project Gutenberg](http://www.gutenberg.org/wiki/Main_Page).  This could also be scaled to larger applications, such as finding the most common words in Wikipedia.
// MAGIC 
// MAGIC ** During this lab we will cover: **
// MAGIC * *Part 1:* Creating a base DataFrame and performing operations
// MAGIC * *Part 2:* Counting with Spark SQL and DataFrames
// MAGIC * *Part 3:* Finding unique words and a mean value
// MAGIC * *Part 4:* Apply word count to a file

// COMMAND ----------

// MAGIC %md
// MAGIC #### ** Part 1: Creating a base DataFrame and performing operations **
// MAGIC 
// MAGIC %md
// MAGIC ** (1a) Create a DataFrame **
// MAGIC 
// MAGIC We'll start by generating a base DataFrame by using a Scala list of tuples and the `sparkcontext.parallelize` method.  Then we'll print out the type and schema of the DataFrame.  The Scala API has several examples for using the [`parallelize` method](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.SparkContext).

// COMMAND ----------

val wordsList = List("cat", "elephant", "rat", "rat", "cat")
val wordsRDD = sc.parallelize(wordsList)


// COMMAND ----------

// MAGIC %md
// MAGIC ### (1b) Pluralize and test
// MAGIC 
// MAGIC Let's use a `map()` transformation to add the letter 's' to each string in the base RDD we just created. We'll define a Scala function that returns the word with an 's' at the end of the word.  Please replace `<FILL IN>` with your solution.  If you have trouble, the next cell has the solution.  After you have defined `makePlural` you can run the third cell which contains a test.  If you implementation is correct it will print `1 test passed`.
// MAGIC 
// MAGIC This is the general form that exercises will take, except that no example solution will be provided.  Exercises will include an explanation of what is expected, followed by code cells where one cell will have one or more `<FILL IN>` sections.  The cell that needs to be modified will have `# TODO: Replace <FILL IN> with appropriate code` on its first line.  Once the `<FILL IN>` sections are updated and the code is run, the test cell can then be run to verify the correctness of your solution.  The last code cell before the next markdown section will contain the tests.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code


def makePlural(word:String) : String = {
    /*Adds an 's' to `word`.

    Note:
        This is a simple function that only adds an 's'.  No attempt is made to follow proper
        pluralization rules.

    Args:
        word (str): A string.

    Returns:
        str: A string with 's' added to it.
    */
    return <FILL IN>
}

print(makePlural("cat"))
print("\n")

// COMMAND ----------

// MAGIC %md
// MAGIC ### (1c) Apply `makePlural` to the base RDD
// MAGIC 
// MAGIC Now pass each item in the base RDD into a [map()](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) transformation that applies the `makePlural()` function to each element. And then call the [collect()](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) action to see the transformed RDD.

// COMMAND ----------

val pluralRDDs = wordsRDD.<FILL IN>
pluralRDDs.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ### (1d) Pass a `lambda` function to `map`
// MAGIC 
// MAGIC Let's create the same RDD using a `lambda` function.

// COMMAND ----------

val pluralRDD = wordsRDD.<FILL IN>
pluralRDD.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ### (1e) Length of each word
// MAGIC 
// MAGIC Now use `map()` and a `lambda` function to return the number of characters in each word.  We'll `collect` this result directly into a variable.

// COMMAND ----------

val pluralLengths = (pluralRDD.<FILL IN>
                 .collect())
print(pluralLengths)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (1f) Pair RDDs
// MAGIC 
// MAGIC The next step in writing our word counting program is to create a new type of RDD, called a pair RDD. A pair RDD is an RDD where each element is a pair tuple `(k, v)` where `k` is the key and `v` is the value. In this example, we will create a pair consisting of `('<word>', 1)` for each word element in the RDD.
// MAGIC We can create the pair RDD using the `map()` transformation with a `lambda()` function to create a new RDD.

// COMMAND ----------

val wordPairs = wordsRDD.<FILL IN>
wordPairs.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2: Counting with pair RDDs

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's count the number of times a particular word appears in the RDD. There are multiple ways to perform the counting, but some are much less efficient than others.
// MAGIC 
// MAGIC A naive approach would be to `collect()` all of the elements and count them in the driver program. While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets. In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers. For these reasons, we will use data parallel operations.

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2a) `groupByKey()` approach
// MAGIC An approach you might first consider (we'll see shortly that there are better ways) is based on using the [groupByKey()](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) transformation. As the name implies, the `groupByKey()` transformation groups all the elements of the RDD with the same key into a single list in one of the partitions.
// MAGIC 
// MAGIC There are two problems with using `groupByKey()`:
// MAGIC   + The operation requires a lot of data movement to move all the values into the appropriate partitions.
// MAGIC   + The lists can be very large. Consider a word count of English Wikipedia: the lists for common words (e.g., the, a, etc.) would be huge and could exhaust the available memory in a worker.
// MAGIC 
// MAGIC Use `groupByKey()` to generate a pair RDD of type `('word', iterator)`.

// COMMAND ----------

val wordsGrouped = wordPairs.<FILL IN>
for ((key, value) <- wordsGrouped.collect())
    println(List(value))

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2b) Use `groupByKey()` to obtain the counts
// MAGIC 
// MAGIC Using the `groupByKey()` transformation creates an RDD containing 3 elements, each of which is a pair of a word and a scala iterator.
// MAGIC 
// MAGIC Now sum the iterator using a `map()` transformation.  The result should be a pair RDD consisting of (word, count) pairs.

// COMMAND ----------

val wordCountsGrouped = wordsGrouped.<FILL IN>
wordCountsGrouped.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ** (2c) Counting using `reduceByKey` **
// MAGIC 
// MAGIC A better approach is to start from the pair RDD and then use the [reduceByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.reduceByKey) transformation to create a new pair RDD. The `reduceByKey()` transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value. `reduceByKey()` operates by applying the function first within each partition on a per-key basis and then across the partitions, allowing it to scale efficiently to large datasets.

// COMMAND ----------

val wordCounts = wordPairs.<FILL IN>
wordCounts.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ### (2d) All together
// MAGIC 
// MAGIC The expert version of the code performs the `map()` to pair RDD, `reduceByKey()` transformation, and `collect` in one statement.

// COMMAND ----------

val wordCountsCollected = (wordsRDD.<FILL IN>)
wordCountsCollected

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 3: Finding unique words and a mean value

// COMMAND ----------

// MAGIC %md
// MAGIC ### (3a) Unique words
// MAGIC 
// MAGIC Calculate the number of unique words in `wordsRDD`.  You can use other RDDs that you have already created to make this easier.

// COMMAND ----------

val uniqueWords = wordCounts.<FILL IN>


// COMMAND ----------

// MAGIC %md
// MAGIC ### (3b) Mean using `reduce`
// MAGIC 
// MAGIC Find the mean number of words per unique word in `wordCounts`.
// MAGIC 
// MAGIC Use a `reduce()` action to sum the counts in `wordCounts` and then divide by the number of unique words.  First `map()` the pair RDD `wordCounts`, which consists of (key, value) pairs, to an RDD of values.

// COMMAND ----------

val totalCount = (wordCounts.<FILL IN>)
val average = totalCount / uniqueWords.toFloat


// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 4: Apply word count to a file

// COMMAND ----------

// MAGIC %md
// MAGIC ### (4a) `wordCount` function
// MAGIC 
// MAGIC First, define a function for word counting.  You should reuse the techniques that have been covered in earlier parts of this lab.  This function should take in an RDD that is a list of words like `wordsRDD` and return a pair RDD that has all of the words and their associated counts.

// COMMAND ----------

def wordCount(wordListRDD:org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[(String,Int)] = {


    /*Creates a pair RDD with word counts from an RDD of words.

    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    */
    return wordListRDD.<FILL IN>
}
wordCount(wordsRDD).collect()


// COMMAND ----------

// MAGIC %md
// MAGIC ### (4b) Capitalization and punctuation
// MAGIC 
// MAGIC Real world files are more complicated than the data we have been using in this lab. Some of the issues we have to address are:
// MAGIC   + Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
// MAGIC   + All punctuation should be removed.
// MAGIC   + Any leading or trailing spaces on a line should be removed.
// MAGIC 
// MAGIC Define the function `removePunctuation` that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.  Use the Python [re](https://docs.python.org/2/library/re.html) module to remove any text that is not a letter, number, or space. Reading `help(re.sub)` might be useful.
// MAGIC If you are unfamiliar with regular expressions, you may want to review [this tutorial](https://developers.google.com/edu/python/regular-expressions) from Google.  Also, [this website](https://regex101.com/#python) is  a great resource for debugging your regular expression.

// COMMAND ----------

import scala.util.matching.Regex
def removePunctuation(text : String): String = {
    /*Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    */
    val pattern = new Regex("[a-zA-Z0-9 ]")
    return <FILL IN>
}
println(removePunctuation("Hi, you!"))
println(removePunctuation("No under_score!"))
println(removePunctuation(" *      Remove punctuation then spaces  * "))




// COMMAND ----------

// MAGIC %md
// MAGIC ### (4c) Load a text file
// MAGIC 
// MAGIC For the next part of this lab, we will use the [Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) from [Project Gutenberg](http://www.gutenberg.org/wiki/Main_Page). To convert a text file into an RDD, we use the `SparkContext.textFile()` method. We also apply the recently defined `removePunctuation()` function using a `map()` transformation to strip out the punctuation and change all text to lower case.  Since the file is large we use `take(15)`, so that we only print 15 lines.

// COMMAND ----------


val fileName = "dbfs:/" + "databricks-datasets/cs100/lab1/data-001/shakespeare.txt"

val shakespeareRDD = sc.textFile(fileName, 8).map(removePunctuation)
shakespeareRDD.take(15)

/*print '\n'.join(shakespeareRDD
                .zipWithIndex()  # to (line, lineNum)
                .map(lambda (l, num): '{0}: {1}'.format(num, l))  # to 'lineNum: line'
                .take(15))*/

// COMMAND ----------

// MAGIC %md
// MAGIC ### (4d) Words from lines
// MAGIC 
// MAGIC Before we can use the `wordcount()` function, we have to address two issues with the format of the RDD:
// MAGIC   + The first issue is that  that we need to split each line by its spaces. ** Performed in (4d). **
// MAGIC   + The second issue is we need to filter out empty lines. ** Performed in (4e). **
// MAGIC 
// MAGIC Apply a transformation that will split each element of the RDD by its spaces. For each element of the RDD, you should apply Python's string [split()](https://docs.python.org/2/library/string.html#string.split) function. You might think that a `map()` transformation is the way to do this, but think about what the result of the `split()` function will be.
// MAGIC 
// MAGIC > Note:
// MAGIC > * Do not use the default implemenation of `split()`, but pass in a separator value.  For example, to split `line` by commas you would use `line.split(',')`.

// COMMAND ----------

val shakespeareWordsRDD = shakespeareRDD.<FILL IN>
val shakespeareWordCount = shakespeareWordsRDD.count()
shakespeareWordsRDD.top(5)
//println(shakespeareWordCount)

// COMMAND ----------

// MAGIC %md
// MAGIC ** (4e) Remove empty elements **
// MAGIC 
// MAGIC The next step is to filter out the empty elements.  Remove all entries where the word is `''`.

// COMMAND ----------

val shakeWordsRDD = shakespeareWordsRDD.<FILL IN>
val shakeWordCount = shakeWordsRDD.count()
println(shakeWordCount)

// COMMAND ----------

// MAGIC %md
// MAGIC ### (4f) Count the words
// MAGIC 
// MAGIC We now have an RDD that is only words.  Next, let's apply the `wordCount()` function to produce a list of word counts. We can view the top 15 words by using the `takeOrdered()` action; however, since the elements of the RDD are pairs, we need a custom sort function that sorts using the value part of the pair.
// MAGIC 
// MAGIC You'll notice that many of the words are common English words. These are called stopwords. In a later lab, we will see how to eliminate them from the results.
// MAGIC Use the `wordCount()` function and `takeOrdered()` to obtain the fifteen most common words and their counts.

// COMMAND ----------

val top15WordsAndCounts = (wordCount(shakeWordsRDD)).takeOrdered(15)(Ordering[Int].reverse.on(x => x._2))

