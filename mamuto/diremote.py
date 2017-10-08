import execnet
import pickle
from itertools import zip_longest, chain, repeat

class ZipExhausted(Exception):
    pass

def zip_default(*args):
    """Iterate args where args are uneven sized iterables. 
    Iterates until the longest arg is exhausted. Missing
    values are filled-in with the first element of the relevant arg.
    """
    counter = len(args) - 1
    def sentinel(default):
        nonlocal counter
        if not counter:
            raise ZipExhausted
        counter -= 1
        yield default
    iterators = [chain(it, sentinel(it[0]), repeat(it[0])) for it in args]
    try:
        while iterators:
            yield tuple(map(next, iterators))
    except ZipExhausted:
        pass

def loadstuff(modules):
    """Import modules needed by the worker functions."""
    modic = {}
    for m in modules:
        if m not in modic:
            modic[m] = __import__(m)
    return modic

def setfunction(fname, modulesdic):
    """Search for function in modules by name and return it."""
    fmod = fname.rsplit(".",1)
    if fmod[0] in modulesdic:
        function = getattr(modulesdic[fmod[0]], fmod[1])
    else:
        function = None
    return function


if __name__ == '__channelexec__':
    function_dictionary = {}
    arguments_dictionary = {}
    while not channel.isclosed():
        id_job, msg, job = pickle.loads(channel.receive())
        if msg == "setup":
            remotemodules = job
            modules = loadstuff(remotemodules)
            channel.send(pickle.dumps([id_job, "ready"]))
        if msg == "add_function":
            function_name = job
            if function_name in function_dictionary:
                channel.send(pickle.dumps([id_job, "pass"]))
            else:
                function_dictionary[function_name] = setfunction(function_name, modules)
                arguments_dictionary[function_name] = [False]
                channel.send(pickle.dumps([id_job, "done"]))
        if msg == "add_args":
            function_name = job[1]
            args = job[0]
            if function_name in function_dictionary:
                arguments_dictionary[function_name] = args
                channel.send(pickle.dumps([id_job, "done"]))
            else:
                channel.send(pickle.dumps([id_job, "pass"]))
        if msg == "compute":
            function_name = job[1]
            args = job[0]           
            args = [x if x[0] is not False else y for y,x in zip_longest(arguments_dictionary[function_name], args,  fillvalue=False)]
            function = function_dictionary[function_name]
            r = [function(*a) for a in zip_default(*args)]
            channel.send(pickle.dumps([id_job, r]))