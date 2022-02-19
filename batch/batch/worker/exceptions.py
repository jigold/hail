

class ImageCannotBePulled(Exception):
    pass


class ImageNotFound(Exception):
    pass


class ContainerDeletedError(Exception):
    pass


class ContainerTimeoutError(Exception):
    pass


class StepNotRunnableError(Exception):
    pass


class JVMUserError(Exception):
    pass
