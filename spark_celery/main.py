from celery import current_app
from celery.bin import worker
import os, copy

default_options = {
    'loglevel': 'INFO',
    # only one SparkContext is allowed in the driver: concurrency has to come from the deployment (on YARN, etc)
    'concurrency': 1,
    # make sure hostname is unique, even if on the same node:
    'hostname': '%h-' + str(os.getpid()),
}

def main(options={}):
    """
    Be a worker, with the given options as if present on the command line.
    """
    app = current_app._get_current_object()
    wrkr = worker.worker(app=app)

    opts = copy.copy(default_options)
    opts.update(options)

    wrkr.run(**opts)
