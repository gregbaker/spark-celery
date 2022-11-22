from celery import Celery


def _default_sparkconf_builder():
    """
    Build a SparkConf object that can be used for the worker's SparkContext.
    """
    from pyspark import SparkConf
    return SparkConf().setAppName('SparkCeleryTask') \
        .set('spark.dynamicAllocation.minExecutors', 1) \
        .set('spark.dynamicAllocation.executorIdleTimeout', 60) \
        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', 3600)


class SparkCeleryApp(Celery):
    loader_cls = 'spark_celery.loader:Loader'
    spark_log_level = 'WARN'

    def __init__(self, sparkconf_builder=None, *args, **kwargs):
        super(SparkCeleryApp, self).__init__(*args, **kwargs)
        self.sparkconf_builder = sparkconf_builder

    def worker_init(self, loader):
        """
        Initialize Spark config and context now.
        """
        from pyspark.sql import SparkSession
        sparkconf_builder = self.sparkconf_builder or _default_sparkconf_builder
        self.spark_conf = sparkconf_builder()
        self.spark = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel(self.spark_log_level)
