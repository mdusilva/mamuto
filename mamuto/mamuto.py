import execnet
#import logging
import pickle
import json
import configparser
from types import FunctionType
from itertools import islice, chain, repeat
#import diremote
from mamuto import diremote
import time

#logger = logging.getLogger(__name__)

def _getfname(function):
    """Returns the name of a function or method as a string"""
    mod = function.__module__
    if type(function) == FunctionType:
        return mod + '.' + function.__name__
    else:
        raise TypeError("Input function must be a user defined FunctionType or MethodType")

def create_config_file(filename="cluster.cfg", hosts={'localhost':2}, workdir="", python="", nice=0):
    """Create configation file with parameters to create execnet gateways"""
    config = configparser.ConfigParser()
    config['parameters'] = {}
    parameters = config['parameters']
    parameters['Hosts'] = json.dumps(hosts)
    parameters['WorkDir'] = workdir
    parameters['Python'] = python
    parameters['Nice'] = str(nice)
    with open(filename, 'w') as configfile:
        config.write(configfile)

def _load_config_file(filename="cluster.cfg"):
    """Load configuration file"""
    config = configparser.ConfigParser()
    config.read(filename)
    parameters = config['parameters']
    hosts = json.loads(parameters['Hosts'])
    workdir = parameters['WorkDir']
    python = parameters['Python']
    nice = int(parameters['Nice'])
    return [hosts, workdir, python, nice]

def _split_every(n, iterable):
    """Create chunks of size n from iterable. Credits: https://stackoverflow.com/users/13169/roberto-bonvallet"""
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

class ZipExhausted(Exception):
    pass

def _zip_default(*args):
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

class Mapper(object):
    """
    Map on cluster
    """

    def __init__(self, configfile, depends=None):
        """
        Prepare the cluster to handle the communications
        
        """
        self.depends = []
        self.function_dictionary = {}
        self.arguments_dictionary = {}
        if depends:
            self.depends = self.depends + depends
        self.setup_cluster(configfile, self.depends)


    def setup_cluster(self, configfile, depends=None):
        """
        Setup a cluster and initialize the remote processes
        """
        hosts, working_dir, python, nice =  _load_config_file(filename=configfile)
        #logger.info('Setting up cluster in hosts: %s' % ', '.join(hosts))
        self.gw = []
        self.channels = []
        self.n_parts = 0
        for host, count in hosts.items():
            self.n_parts = self.n_parts + count
            for i in range(count):
                self.gw.append(execnet.makegateway('ssh=%s//nice=%d//chdir=%s//python=%s' % (host, nice, working_dir, python)))
                #popen option
                #self.gw.append(execnet.makegateway('popen//nice=%d//chdir=%s//python=%s' % (nice, working_dir, python)))
                self.channels.append(self.gw[i].remote_exec(diremote))
        self.mch = execnet.MultiChannel(self.channels)
        #logger.info('Sending dependencies')
        self._sendjobs("setup", depends)
        self.queue = self.mch.make_receive_queue(endmarker=-1)
        results = self._receiveresults()
        #logger.info('Remote output: %s' % ', '.join(results))

    def add_function(self, function):
        """Add function to remote worker processes"""
        function_name = _getfname(function)
        self.function_dictionary[function_name] = function
        #logger.info('Sending function name and setting up in remote processes')
        self._sendjobs("add_function", function_name)
        results = self._receiveresults()
        #logger.info('Remote output: %s' % ', '.join(results))

    def add_remote_arguments(self, function, args):
        """Add fixed arguments to a given function in remote worker processes"""
        function_name = _getfname(function)
        if function_name in  self.function_dictionary:
            modified_args = [[False] if a is None else a for a in args]
            #logger.info('Sending fixed arguments in function to remote processes')
            self._sendjobs("add_args", [modified_args, function_name])
            results = self._receiveresults()
            #logger.info('Remote output: %s' % ', '.join(results))
        else:
            #logger.error('Function is not known, not doing anything')
             raise ValueError("Function not included in function dictionary: use 'add_function' method")

    def remap(self, function, args):
        """Remote execution version of map: map arguments with function"""
        function_name = _getfname(function)
        if function_name in  self.function_dictionary:
            modified_args = [[False] if a is None else a for a in args]
            #logger.info('Sending arguments and computing function in remote processes')
            self._sendjobs("compute", [modified_args, function_name])
            results = self._receiveresults()
            #logger.debug('Remote output: %s' % ', '.join(str(results)))
            return list(chain.from_iterable(results))
        else:
            #logger.error('Function is not known, not doing anything')
            raise ValueError("Function not included in function dictionary: use 'add_function' method")
        
    def _sendjobs(self, msg, job):
        """Send jobs to worker processes"""
        if msg == "setup" or msg == "add_function":
            for i in range(self.n_parts):
                #logger.debug('Setting up gateway %s', str(self.mch[i].gateway.id))
                self.mch[i].send(pickle.dumps([i, msg, job]))
        elif msg == "add_args" or msg == "compute":
            chunks = ([c for c in _split_every(int(len(a)/self.n_parts)+(len(a) % self.n_parts > 0), a)] for a in job[0])
            args = [list(n) for n in _zip_default(*chunks)]
            for i in range(self.n_parts):
                #logger.debug('Sending  data to gateway %s', str(self.mch[i].gateway.id))
                self.mch[i].send(pickle.dumps([i, msg, (args[i], job[1])]))
    
    def _receiveresults(self):
        """Receive from remore worker processes in a queue"""
        finished_jobs = 0
        results = []
        #logger.debug('Starting to receive data from remote hosts...')
        while True:
            channel, item = self.queue.get()
            #logger.debug('Receiving data in channel %s from gateway %s',str(channel), str(channel.gateway.id))
            if item is not -1:
                r = pickle.loads(item)
                results.append(r)
                finished_jobs = finished_jobs + 1
            if finished_jobs == self.n_parts: break
        #logger.debug('Finished receiving data from remote hosts.')
        results.sort()
        results = [x[1] for x in results]
        return results
    