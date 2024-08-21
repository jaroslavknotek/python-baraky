


class MinioQueue:
    def __init__(self, storage):
        self.storage = storage

    def put(self, message:str):    
        self.storage.save(message)
        pass

    def pop(self):
        pass
