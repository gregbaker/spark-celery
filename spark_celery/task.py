from celery.app.task import Task
from celery.utils import abstract


@abstract.CallableTask.register
class SparkCeleryTask(Task):
    name = None

    def __init__(self, *args, **kwargs):
        super(SparkCeleryTask, self).__init__(*args, **kwargs)
        if not self.name:
            raise(ValueError, 'Task name must be set explicitly.')
