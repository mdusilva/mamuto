"""
Mamuto
======

This package implements an improved map method that works over a computer cluster.
It is very simple, requiring minimal input or configuration from the user. The package 
[execnet](http://codespeak.net/execnet/index.html) is used to launch Python processes 
and distribute tasks to local and/or remote CPUs through ssh.

Features
--------

* Apply functions to items in large lists or Numpy arrays using remote or local
processes.
* Send just some or all of the arguments only once to the remote processes. Useful when many calls 
are made, as it avoids sending the same data repeatedly.
* Easily configure your cluster environment using configuration files which can be created by a simple 
function call.

Usage
-----

The functions that we want to compute must be part of a module and the module must be
importable by the worker processes.

To use this package, the first step should be to create a configuration file for your
cluster environment. To do this use the `create_config_file` function:

```python
import mamuto

mamuto.create_config_file(filename="cluster.cfg", hosts={'localhost':4, 'rem1':4}, workdir="", python="/home/user/anaconda3/envs/env1/bin/python", nice=0)
```

This creates a configuration file called "cluster.cfg" defining a cluster with two
nodes (with hostnames loclhost and rem1) and 4 worker processes in each
(this number should ideally be <= number of cores in the node). The working directory
is left blank to indicate to use the current directory and the Python interpreter to use
is from an anaconda virtual environment and has the full path indicated.

To compute a function `bar` defined in module "foo.py", first create an instance of a `Mapper` object:

```python
m1 = mamuto.Mapper(configfile, depends=["foo"])
```

Then tell the worker processes to prepare to compute the function `bar` so they make the
necessary imports:

```python
m1.add_function(foo.bar)
```

The arguments must be put on a list and must be either iterables (all with the same length)
or single elements. For example if the function `bar` has three arguments:

```python
a = [1,2,3,4]
b = 5
c = [6,7,8,9]
args = [a, b, c]
```

Finally to map the arguments in the list `args` use the `remap` method:

```python
result = m1.remap(foo.bar, args)
```

Some or all arguments can be sent to the worker processes beforehand using the 
`add_remote_arguments`method, for example to send the first two arguments:

```python
a = [1,2,3,4]
b = 5
c = [6,7,8,9]
args = [a, b, None]

m1.add_remote_arguments(bar, args)
```

The to map the function we need to send just the missing third argument:

```python
args = [None, None, c]

result = m1.remap(foo.bar, args)
```

"""
#from distopy.distopy import getfname
#from mamuto import mamuto
from mamuto.mamuto import create_config_file, Mapper
#import logging
#logging.basicConfig(filename='mammoth.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.CRITICAL)

__version__ = "0.1"