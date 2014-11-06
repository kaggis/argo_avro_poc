#!/usr/bin/env python

# arg parsing related imports
from argparse import ArgumentParser
import argparse, os

# avro related imports 
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


### ARGUMENT PARSING

def file_valid(x):
	if not os.path.exists(x):
		raise argparse.ArgumentTypeError("{0} is not a valid file!".format(x))
	return x

arg_parser = ArgumentParser(description="Decoding of argo avro-binary files")
arg_parser.add_argument("-i","--input",help="avro binary file", dest="file_in", metavar="FILE", required="TRUE", type=file_valid)

args = arg_parser.parse_args()
