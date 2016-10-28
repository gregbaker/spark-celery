# spark-submit --master=yarn-client demo.py
# CONSUMER_PRIORITY=5 spark-submit --master=yarn-client demo.py

from spark_celery import SparkCeleryApp, SparkCeleryTask, RDD_builder, main

BROKER_URL = 'amqp://ggbaker:rabbitmqpassword@localhost:5672/ggbakervhost'
BACKEND_URL = 'rpc://'

def sparkconfig_builder():
    from pyspark import SparkConf
    return SparkConf().setAppName('SparkCeleryTask') \
        .set('spark.dynamicAllocation.enabled', 'true') \
        .set('spark.dynamicAllocation.schedulerBacklogTimeout', 1) \
        .set('spark.dynamicAllocation.minExecutors', 1) \
        .set('spark.dynamicAllocation.executorIdleTimeout', 20) \
        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', 60)

app = SparkCeleryApp(broker=BROKER_URL, backend=BACKEND_URL, sparkconfig_builder=sparkconfig_builder)


# Setting priority for workers allows primary workers, with spillover if the primaries are busy. Used to minimize the
# number of Spark contexts (on the cluster, or caching common data). Works only with Celery >= 4.0
import os
from kombu import Queue
priority = int(os.environ.get('CONSUMER_PRIORITY', '10'))
app.conf['CELERY_QUEUES'] = (
    Queue('celery', consumer_arguments={'x-priority': priority}),
)


@app.task(bind=True, base=SparkCeleryTask, name='simple_sum')
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
    name = 'tasks.WordCount'

    @RDD_builder
    def get_data(self, inputs):
        """
        Build RDD of wordcounts from the inputs directory, sorted by decreasing count.
        """
        text = app.sc.textFile(inputs)
        words = text.flatMap(lambda line: line.split()).map(lambda w: (w, 1))
        wordcount = words.reduceByKey(operator.add).sortBy(lambda (w,c): (-c,w)).cache()
        return wordcount

    def run(self, inputs, first_letter):
        """
        Return 10 most common words from the input directory that start with first_letter.
        """
        wordcount = self.get_data(inputs)
        first_letter = first_letter.lower()
        with_first = wordcount.filter(lambda (w,c): w[0].lower() == first_letter)
        return with_first.take(10)

app.tasks.register(WordCount())


if __name__ == '__main__':
    # When called as a worker, run as a worker.
    main()
