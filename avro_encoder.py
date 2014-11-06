#!/usr/bin/env python

from argparse import ArgumentParser
import argparse, os

def file_valid(x):
	if not os.path.exists(x):
		raise argparse.ArgumentTypeError("{0} is not a valid file!".format(x))
	return x
	

def dir_valid(x): 
	d = os.path.dirname(x)
	
	 
	
	if d!= "" and not os.path.exists(d):
		raise argparse.ArgumentTypeError("{0} not on valid directory!".format(x))
	
	return x

	


arg_parser = ArgumentParser(description="Encoding of raw argo files in avro format")
arg_parser.add_argument("-i","--input",help="raw text input file", dest="file_in", metavar="FILE", required="TRUE", type=file_valid)
arg_parser.add_argument("-o","--output",help="output avro file ", dest="file_out", metavar= "FILE", required="TRUE", type=dir_valid)
arg_parser.add_argument("-s","--schema",help="avro schema file", dest="file_schema", metavar="FILE", required="TRUE", type=file_valid)
args = arg_parser.parse_args()

print args