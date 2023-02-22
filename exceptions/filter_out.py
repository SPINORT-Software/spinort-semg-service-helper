class FilterOutException(Exception):
    def __init__(self, consumer_type, reason, *args):
        super().__init__(args)
        self.reason = reason
        self.consumer_type = consumer_type

    def __str__(self):
        return f"The incoming message of [{self.consumer_type}] has been filtered. Reason: [{self.reason}]"
