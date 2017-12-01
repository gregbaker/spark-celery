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

    def __init__(self, sparkconf_builder=None, *args, **kwargs):
        super(SparkCeleryApp, self).__init__(*args, **kwargs)

        self.sparkconf_builder = sparkconf_builder

        if not self._config_source:
            self._config_source = {}

        cfg = self._config_source
        # The fetch is much smaller than the task startup time, so don't worry about it. Prefer not getting stuck behind
        # a long task. Instantiator can still explicitly override if they want.
        if 'CELERYD_PREFETCH_MULTIPLIER' not in cfg:
            cfg['CELERYD_PREFETCH_MULTIPLIER'] = 1

    def worker_init(self, loader):
        """
        Initialize Spark config and context now.
        """
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
        sparkconf_builder = self.sparkconf_builder or _default_sparkconf_builder
        self.spark_conf = sparkconf_builder()
        self.sc = SparkContext(conf=self.spark_conf)
        self.spark = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()