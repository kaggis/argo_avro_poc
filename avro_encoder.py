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
	

def dir_valid(x): 
	d = os.path.dirname(x)
	if d!= "" and not os.path.exists(d):
		raise argparse.ArgumentTypeError("{0} not on valid directory!".format(x))
	
	return x

# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
arg_parser = ArgumentParser(description="Encoding of raw argo files in avro format")
arg_parser.add_argument("-i","--input",help="raw text input file", dest="file_in", metavar="FILE", required="TRUE", type=file_valid)
arg_parser.add_argument("-o","--output",help="output avro file ", dest="file_out", metavar= "FILE", required="TRUE", type=dir_valid)
arg_parser.add_argument("-s","--schema",help="avro schema file", dest="file_schema", metavar="FILE", required="TRUE", type=file_valid)

# Parse the command line arguments accordingly...
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
			print tokens
			print len(tokens)
			print len(schema.fields)
			

		post = {} # dictionary : fields + row data
		for i,val in enumerate(tokens):
			#Convert text according to field type
			if i==len(schema.fields):
				break
			if schema.fields[i].type.type == "int":
				post[schema.fields[i].name]=int(val)
			elif schema.fields[i].type.type == "long":
				post[schema.fields[i].name]=long(val)
			elif schema.fields[i].type.type == "float":
				post[schema.fields[i].name]=float(val)
			elif schema.fields[i].type.type == "double":
				post[schema.fields[i].name]=float(val)
			elif schema.fields[i].type.type == "bool":
				post[schema.fields[i].name]=bool(val)
			else:
				post[schema.fields[i].name]=val

		records.append(post)	


### WRITE RECORDS TO AVRO FILE

avro_write =  DataFileWriter(open(args.file_out, "w"), DatumWriter(), schema)

# iterate on all row data and write it to avro file 
for i,row in enumerate(records):
	avro_write.append(row);

total_rows = i+1 #number of total rows written

# Print operational stats

#file is saved
avro_write.close()

### PRINT STATISTICS

print "schema file:", args.file_schema
print "schema name:", schema.name
print "schema type:", schema.type
print "\nschema num of fields:", len(schema.fields)
for field in schema.fields:
	print "  field:", field.name , "--",field.type.type
print "\ninput file:" ,args.file_in
print "output file:" ,args.file_out
print "total rows written:" ,total_rows
