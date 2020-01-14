import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import StandardOptions, SetupOptions, PipelineOptions

class YourBranchingFn(beam.DoFn):
    def process(self, element):
        if re.match(r'^a', element):
            yield beam.pvalue.TaggedOutput('a', element)
        elif re.match(r'^b', element):
            yield beam.pvalue.TaggedOutput('b', element)
        else:
            yield element

class Check(beam.DoFn):
    def process(self, element):
        logging.info(element)
        return element

def run():
    options = PipelineOptions()

    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DirectRunner'
    standard_options.streaming = True

    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    p = beam.Pipeline(options=options)

    # branching example 1
    p2 = (
        p
        | "ReadText1" >> beam.Create(['aaa', 'bbb', 'ccc'])
        | "Branching1" >> beam.ParDo(YourBranchingFn()).with_outputs('a', 'b', main='m')
    )

    p2.a | "Check p2.a" >> beam.ParDo(Check())
    p2.b | "Check p2.b" >> beam.ParDo(Check())
    p2.m | "Check p2.m" >> beam.ParDo(Check())
    p2.a | "WriteToText p2.a" >> beam.io.WriteToText('./a.txt')
    p2.b | "WriteToText p2.b" >> beam.io.WriteToText('./b.txt')
    p2.m | "WriteToText p2.m" >> beam.io.WriteToText('./main.txt')

    # branching example 2
    m, a, b = (
        p | "ReadText2" >> beam.Create(['aaa2', 'bbb2', 'ccc2'])
          | "Branching2" >> beam.ParDo(YourBranchingFn()).with_outputs('a', 'b', main='m')
    )

    a | "Check a" >> beam.ParDo(Check())
    b | "Check b" >> beam.ParDo(Check())
    m | "Check m" >> beam.ParDo(Check())
    a | "WriteToText a" >> beam.io.WriteToText('./a.txt')
    b | "WriteToText b" >> beam.io.WriteToText('./b.txt')
    m | "WriteToText m" >> beam.io.WriteToText('./main.txt')

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
