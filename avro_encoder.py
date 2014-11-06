#!/usr/bin/env python

from argparse import ArgumentParser
import argparse, os
import avro.schema


### ARGUMENT PARSING

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

### READ SCHEMA 
schema = avro.schema.parse(open(args.file_schema).read())

### READ RAW FILE AND CREATE DIC ACCORDING TO SCHEMA

records = [] # store the collection of row data
 
with open(args.file_in) as f:
	for line in f:
		tokens = line.split('\001') # Split each row into fields
		# Check if number of fields in line == number of fields in schema
		if len(tokens) != len(schema.fields):
			print "Mismatch of number of fields between raw file and schema!" 
			exit(1)

		post = {} # dictionary : fields + row data
		for i,val in enumerate(tokens):
			post[schema.fields[i].name]=val

		records.append(post)	

print records

