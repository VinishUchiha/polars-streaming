class FileSourcePathMissingException(Exception):
    def __init__(self,message):
        self.message = message

class FileExtensionNotSupportedException(Exception):
    def __init__(self,message):
        self.message = message

class NotImplementedError(Exception):
    def __init__(self,message):
        self.message = message

class ModuleNotFoundException(Exception):
    def __init__(self,message):
        self.message = message