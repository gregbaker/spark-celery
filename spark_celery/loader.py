from celery.loaders.app import AppLoader

class Loader(AppLoader):
    def on_worker_init(self):
        # call back to the app when its worker is started, so it can initialize itself.
        self.app.worker_init(self)