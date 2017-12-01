from distutils.core import setup
setup(
  name = 'spark_celery',
  packages = ['spark_celery'],
  version = '0.1.1',
  description = 'A helper to allow Python Celery tasks to do work in a Spark job',
  author = 'Greg Baker',
  author_email = 'ggbaker@sfu.ca',
  url = 'https://github.com/gregbaker/spark-celery',
  download_url = 'https://codeload.github.com/gregbaker/spark-celery/zip/master',
  keywords = ['spark', 'celery', 'bigdata'],
  install_requires=['celery', 'kombu'],
  classifiers = [
      'Development Status :: 4 - Beta',
      'License :: OSI Approved :: Apache Software License',
      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 3',
      'Operating System :: OS Independent',
      'Topic :: System :: Distributed Computing',      
  ],
)