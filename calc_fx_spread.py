#! /usr/bin/env python

import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from sys import argv

# Use Python argparse module to parse custom arguments

parser = argparse.ArgumentParser()
parser.add_argument('--input')
parser.add_argument('--output')
known_args, pipeline_args = parser.parse_known_args(argv)

# Create a Pipeline
p = beam.Pipeline(argv=pipeline_args)

input_file = known_args.input
output_file = known_args.output

# Process method is called for each row
class Split(beam.DoFn):
    def process(self, element):
        Date,Bid,Ask,Volume = element.split(",")
        return [{
            'DateTime' : Date,
            'Bid' : float(Bid),
            'Ask' : float(Ask),
        }]

# return as list, else it wont work... error "TypeError: 'float' object is not iterable "
class CalculateSpread(beam.DoFn):
    def process(self, element):
        result = [element['Ask'] - element['Bid']]
        return result

# Chain all the transforms
tickData = ( 
           p | 'ReadMyFile' >> beam.io.ReadFromText(input_file) | 
           beam.ParDo(Split()) |
           beam.ParDo(CalculateSpread()) |
           beam.io.WriteToText(output_file)
)

result = p.run()
result.wait_until_finish()

# execute using local runner
# ./calc_fx_spread.py --input='/Users/bipulk/gcp/dataflow/df/data/DAT_ASCII_EURGBP_T_201811.csv' \
#                     --output='/Users/bipulk/gcp/dataflow/df/data/DAT_ASCII_EURGBP_T_201811_Spread.txt'

#  execute using Dataflow runner
# ./calc_fx_spread.py --input=gs://bipul-df/DAT_ASCII_EURGBP_T_201811.csv \
#                     --output=gs://bipul-df/DAT_ASCII_EURGBP_T_201811_Spread.txt \
#                     --runner DataflowRunner \
#                     --project=bipul-test-001 \
#                     --temp_location gs://bipul-df/tmp
