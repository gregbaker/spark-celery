# spark-celery

A helper to allow Python Celery tasks to do work in a Spark job. Highlights:

* Celery tasks can do work inside Spark jobs running on a huge cluster or just in a local test environment.
* Celery tasks start up quickly, since the Celery worker keeps the SparkContext alive, ready to do work.
* Cached RDDs can be shared between Celery tasks, so intermediate results don't get calculated with every call.
* Spark libraries are **not** required where you're submitting the Celery tasks, only where you're running Spark jobs.

All of this is done by having a Celery worker running inside a Spark application. Each Celery task becomes a Spark job within that application.


## Prerequisites

You need somewhere to run Spark applications: this has been tested on YARN and local.

You will need a [Celery broker](http://docs.celeryproject.org/en/latest/getting-started/brokers/) that is accessible both from your cluster (or wherever you're running Spark)
and from  where you want to call Celery tasks.

[RabbitMQ](https://www.rabbitmq.com/) is probably the best choice. You need to [install it](http://docs.celeryproject.org/en/latest/getting-started/brokers/rabbitmq.html)
(which involves installing the ```rabbitmq``` package on most Linux flavours, and running a few ```rabbitmqctl``` commands to set up access).


## Spark Celery Worker

The magic that happens here is that you can submit a Spark application which acts as a Celery worker.
The worker consumes tasks in the job queue and executes them, within the Spark applicaton.
Since the Spark app is already running, you can get computation started quickly and your results back in a fraction of a second
(plus the time to actually compute the results: we can't avoid that for you).

Assuming a RabbitMQ setup as above, the Celery app can be created like this. There is a ```demo.py``` in this repository that can serve as an example.

```python
BROKER_URL = 'amqp://myuser:mypassword@localhost:5672/myvhost'
app = SparkCeleryApp(broker=BROKER_URL, backend='rpc://')
```

The worker can be started inside a Spark job, started something like this:

```bash
spark-submit --master=local[*] demo.py
spark-submit --master=yarn-client demo.py
```

## Calling Tasks

With any luck, you can now submit tasks from anywhere you can connect to your Celery backend. To the caller, they should work like any other Celery task.

```python
>>> from demo import *
>>> res = simple_sum.delay(100000)
>>> res.wait()
4999950000
>>> res = WordCount().delay('wordcount', 'a')
>>> res.wait()
[[u'and', 77130], [u'a', 32121], [u'And', 15925], [u'as', 13177], [u'all', 11706], [u'at', 8532], [u'are', 6385], [u'an', 4684], [u'any', 3247], [u'am', 2571]]
```

