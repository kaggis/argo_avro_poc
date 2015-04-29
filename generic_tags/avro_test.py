#!/usr/bin/env python

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("group_endpoints_test.avsc").read())
writer = DataFileWriter(open("groups_endpoints_test.avro", "w"), DatumWriter(), schema)

writer.append({"type": "ngi", "group": "ngi_foo", "service": "CREAM-CE", "hostname":"foo1"})
writer.append({"type": "ngi", "group": "ngi_foo", "service": "CREAM-CE", "hostname":"foo2", "tags":{"tag_a":"value_a","tag_b":"value_b"}})
writer.append({"type": "ngi", "group": "ngi_foo", "service": "CREAM-CE", "hostname":"foo3", "tags":{"tag_c":"value_c","tag_d":"value_d","tag_e":"value_e"}})
writer.close()