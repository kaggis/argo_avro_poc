argo_avro_poc
=============

investigating apache avro usage in argo


dependencies
------------

#### Module: avro 
Avro is a serialization and RPC framework.

`pip install avro`

#### Module: argparse (only if python<2.7)
Python command-line parsing library

`pip install argparse`


folders
-------

###raw files/
contains raw text files of argo sync components

e.g.

####downtime-example.raw
a sample of downtime sync text file

###avro files/
contains converted avro binary files 

e.g.

####downtime-example.avro
avro binary converted file from downtime-sample.raw text

###schemas/
e.g.

####downtimes.avsc
avro defined schema of downtime objects

executables
-----------

###avro_encoder.py
a python script that takes as input:
- a raw text file
- an avro schema 

and produces an avro binary file as output

example usage:

`./avro_encoder.py -i raw_files/downtime-example.raw -s schemas/downtimes.avsc -o avro_files/downtime-example.avro` 

-i input text file

-o output avro binary file

-s avro schema file

    
###avro_decoder.py
a python script that takes as input:
- an avro encoded file

exctracts the included schema and produces json output

example usage:

`./avro_decoder.py -i avro_files/downtime-example.avro -o downtime.json`

-i input file

-o output json file