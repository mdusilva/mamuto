"""
Mamuto
======

This package implements an improved map method that works over a computer cluster.
It is very simple, requiring minimal input or configuration from the user. The package 
[execnet](http://codespeak.net/execnet/index.html) is used to launch Python processes 
and distribute tasks to local and/or remote CPUs through ssh.

Features
--------

* Apply functions to items in very large lists or Numpy arrays using remote or local
processes.
* Send just some or all of the arguments only once to the remote processes. Useful when many calls 
are made, as it avoids sending the same data repeatedly.
* Easily configure your cluster environment using configuration files which can be created by a simple 
function call.



"""

#from distopy.distopy import getfname
#from mamuto import mamuto
from mamuto.mamuto import create_config_file, Mapper
#import logging
#logging.basicConfig(filename='mammoth.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.CRITICAL)

__version__ = "0.1"