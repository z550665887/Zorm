class FieldError(Exception):
    def __init__(self, args):
        self.args = args
   # def __str__(self):
   #      return self.args

class NotFindArgv(Exception):
    def __init__(self, args):
        self.args = args
   # def __str__(self):
        # return self.args