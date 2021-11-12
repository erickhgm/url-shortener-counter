import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration

from google.cloud import firestore

from transforms import count_ones
from transforms import FormatAsDictFn
from transforms import FireStoreUpdateFn


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True, help=('Google Cloud project id'))
    parser.add_argument('--collection', required=True, help=('Firestore collection'))
    parser.add_argument('--fixed_windows', required=True, help=('Size of the window in seconds'))
    parser.add_argument('--input_subscription', required=True, help=('Input PubSub subscription'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        ids = (
            p
            | 'read_from_pub_sub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription).with_output_types(bytes)
            | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
        )
        
        size = Duration(seconds=int(known_args.fixed_windows))

        counts = (
            ids
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'fixed_windows' >> beam.WindowInto(window.FixedWindows(size=size, offset=0))
            | 'group_by_key' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones)
        )

        (
            counts
            | 'format_dict' >> beam.ParDo(FormatAsDictFn())
            | 'update_database' >> beam.ParDo(FireStoreUpdateFn(project_id=known_args.project_id, collection=known_args.collection))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
