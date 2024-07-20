import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read lines' >> beam.io.ReadFromText("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark-prep/example.txt")
        | "Convert to Upper" >> beam.Map(lambda x:x.upper())
        | "Output to file" >> beam.io.WriteToText("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark-prep/example1.txt")
    )