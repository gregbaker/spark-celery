from spark_celery import SparkCeleryApp, SparkCeleryTask, cache, main

BROKER_URL = 'amqp://myuser:mypassword@localhost:5672/myvhost'
BACKEND_URL = 'rpc://'

def sparkconfig_builder():
    from pyspark import SparkConf
    return SparkConf().setAppName('SparkCeleryTask') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.dynamicAllocation.schedulerBacklogTimeout', 1) \
        .set('spark.dynamicAllocation.minExecutors', 1) \
        .set('spark.dynamicAllocation.executorIdleTimeout', 20) \
        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', 60)

app = SparkCeleryApp(broker=BROKER_URL, backend=BACKEND_URL, sparkconf_builder=sparkconfig_builder)


# Setting priority for workers allows primary workers, with spillover if the primaries are busy. Used to minimize the
# number of Spark contexts (active on the cluster, or caching common data).
# Run a lower-priority consumer like this:
#   CONSUMER_PRIORITY=5 spark-submit --master=yarn-client demo.py
import os
from kombu import Queue
priority = int(os.environ.get('CONSUMER_PRIORITY', '10'))
app.conf['task_queues'] = (
    Queue('celery', consumer_arguments={'x-priority': priority}),
)


@app.task(bind=True, base=SparkCeleryTask, name='demo.simple_sum')
def simple_sum(self, n):
    """
    A simple task function that sums numbers 0 to n-1, using Spark.
    """
    rdd = app.sc.parallelize(range(n), numSlices=(1 + n//1000))
    return rdd.sum()


import operator
class WordCount(SparkCeleryTask):
    """
    Class-based Spark Task example, with a cached RDD shared between calls to the task.
    """
    name = 'demo.WordCount'

    @cache
    def get_data(self, inputs):
        """
        Build RDD of wordcounts from the inputs directory, sorted by decreasing count.
        """
        text = app.sc.textFile(inputs)
        words = text.flatMap(lambda line: line.split()).map(lambda w: (w, 1))
        wordcount = words.reduceByKey(operator.add).sortBy(lambda wc: (-wc[1], wc[0])).cache()
        return wordcount

    def run(self, inputs, first_letter):
        """
        Return 5 most common words from the input directory that start with first_letter.
        """
        wordcount = self.get_data(inputs)
        first_letter = first_letter.lower()
        with_first = wordcount.filter(lambda wc: wc[0][0].lower() == first_letter)
        return with_first.take(5)


app.register_task(WordCount())


import operator
class DataFrameWordCount(SparkCeleryTask):
    """
    Class-based Spark Task example, with a cached DataFrame shared between calls to the task.
    """
    name = 'demo.DataFrameWordCount'

    @cache
    def get_data(self, inputs):
        """
        Build DataFrame of wordcounts from the inputs directory, sorted by decreasing count.
        """
        from pyspark.sql import functions
        text = app.spark.read.text(inputs)
        words = text.select(
            functions.explode(
                functions.split(text['value'], ' ')).alias('word'))
        words = words.filter(words['word'] != '')
        wordcount = words.groupby('word').agg(functions.count('word').alias('count'))
        wordcount = wordcount.orderBy('count', ascending=False)
        return wordcount.cache()

    def run(self, inputs, first_letter):
        """
        Return 5 most common words from the input directory that start with first_letter.
        """
        from pyspark.sql import functions
        wordcount = self.get_data(inputs)
        first_letter = first_letter.lower()
        with_first = wordcount.filter(functions.lower(wordcount['word']).startswith(first_letter))
        return [tuple(r) for r in with_first.take(5)]


app.register_task(DataFrameWordCount())

    
# Scheduling a periodic task can be done in the beat_schedule and will run if you update the call to main to:
# main(options={'beat': True})

#from celery.schedules import timedelta
#app.conf.beat_schedule = {
#    'frequently-count-words': {
#        'task': 'tasks.WordCount',
#        'schedule': timedelta(seconds=10),
#        'args': ('wordcount', 'a',),
#    },
#}

if __name__ == '__main__':
    # When called as a worker, run as a worker.
    main()
