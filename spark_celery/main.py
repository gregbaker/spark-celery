from celery.bin.celery import celery
import os, copy

default_options = {
    '--loglevel': 'INFO',
    # The fetch is much smaller than the task startup time, so don't worry about it.
    # Prefer not getting stuck behind a long task.
    '--prefetch-multiplier': 1,
    # only one SparkContext is allowed in the driver: concurrency has to come from the deployment (on YARN, etc)
    '--concurrency': 1,
    # make sure hostname is unique, even if on the same node:
    '--hostname': '%h-' + str(os.getpid()),
}

def main(options={}, auto_envvar_prefix='CELERY'):
    """
    Be a worker, with the given options as if present on the command line.
    """
    opts = copy.copy(default_options)
    opts.update(options)

    # convert opts dictionary to an argv list
    nested = ((k, str(v)) for k,v in opts.items())
    # ... flattening with https://stackoverflow.com/a/952952
    argv = [item for sublist in nested for item in sublist]

    celery(['worker'] + argv, auto_envvar_prefix=auto_envvar_prefix)
