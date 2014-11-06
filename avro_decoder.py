#!/usr/bin/env python

import json

# arg parsing related imports
from argparse import ArgumentParser
import argparse, os

# avro related imports 
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader



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

arg_parser = ArgumentParser(description="Decoding of argo avro-binary files")
arg_parser.add_argument("-i","--input",help="avro binary file", dest="file_in", metavar="FILE", required="TRUE", type=file_valid)
arg_parser.add_argument("-o","--output",help="output avro file ", dest="file_out", metavar= "FILE", required="TRUE", type=dir_valid)
args = arg_parser.parse_args()

### EXTRACT SCHEMA FROM FILE

reader = DataFileReader(open(args.file_in, "r"), DatumReader())
schema = reader.datum_reader.writers_schema

schema_fields = []

for field in schema.fields:
	schema_fields.append({"name":field.name,"type":field.type.type})

schema_data = {}

schema_data["name"]=str(schema.name)
schema_data["type"]=str(schema.type)
schema_data["fields"] = schema_fields	

### EXTRACT ROW DATA FROM FILE

row_data =[]
for i,row in enumerate(reader):
	row_data.append(row)

total_rows = i+1

### EXPORT TO JSON FILE

json_data = {}
json_data["schema"] = schema_data
json_data["data"] = row_data

with open(args.file_out, 'wb') as fp:
    json.dump(json_data, fp , indent=4, sort_keys=True)

### PRINT STATISTICS

print "schema name:", schema.name
print "schema type:", schema.type
print "\nschema num of fields:", len(schema.fields)
for field in schema.fields:
	print "  field:", field.name , "--",field.type.type
print "\ninput file:" ,args.file_in
print "output file:" ,args.file_out
print "total rows read:" ,total_rows