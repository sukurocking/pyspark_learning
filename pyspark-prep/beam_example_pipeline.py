import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AfterWatermark, AccumulationMode, Repeatedly

options = PipelineOptions()
p = beam.Pipeline(options=options)

events = [
    ('event1', '2024-07-13T10:00:00Z'),
    ('event2', '2024-07-13T10:05:00Z'),
    ('event3', '2024-07-13T10:09:05Z'),
    ('event4', '2024-07-13T10:10:00Z')
]

def to_timestamp(event):
    from apache_beam.utils.timestamp import Timestamp
    return event[0], Timestamp.from_rfc3339(event[1])


# Create windowed PCollection with triggers and accumulation mode
windowed_events = (
    p
    | 'Create events' >> beam.Create(events)
    | 'Add timestamps' >> beam.Map(to_timestamp)
    | 'Window into Fixed Intervals' >> beam.WindowInto(
        beam.window.FixedWindows(300)
    )
    | 'Group by key' >> beam.GroupByKey()
    | 'print' >> beam.Map(print)
)

result = p.run()
result.wait_until_finish()
