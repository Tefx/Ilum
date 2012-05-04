import marshal
from types import FunctionType


class Fun(object):
    def __init__(self, f):
        self.name = f.func_name
        self.f_code = marshal.dumps(f.func_code)
        self.f = None

    def __call__(self, *args):
        if not self.f:
            self.f = FunctionType(marshal.loads(self.f_code), globals())
        return self.f(*args)

    def __repr__(self):
        return "[wrapped function: '%s']" % (self.name,)


class RemoteException(object):
    def __init__(self, exception, expression):
        self.exception = exception
        self.expression = expression

    def __str__(self):
        return 'Exception occurred while evaling "%s":\n\t%s' % (repr(self.expression), self.exception)
