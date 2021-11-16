import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration

from google.cloud import firestore


# Count the occurrences of each id.
def count_ones(element):
    (id, ones) = element
    return (id, sum(ones))

# Format the counts into a PCollection of dict.
class FormatAsDictFn(beam.DoFn):
    def process(self, id_count):
        (id, count) = id_count
        yield {'id': id, 'clicks': count}

# Increment clicks in Firestore.
class FireStoreUpdateFn(beam.DoFn):
    def __init__(self, project_id, collection):
        self.project_id = project_id
        self.collection = collection

    def start_bundle(self):
        logging.debug('Starting FireStore Client ...')
        self._db = firestore.Client(project=self.project_id)

    def process(self, item):
        id = item['id']
        clicks = item['clicks']

        doc_ref = self._db.collection(self.collection).document(id)
        doc_ref.update({"clicks": firestore.Increment(clicks)})
        logging.info(f'Item incremented on FireStore: {item}')


def run(argv=None, save_main_session=True):
    logging.info('Building pipeline ...')

    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', required=True, help=('Google Cloud project id'))
    parser.add_argument('--collection', required=True, help=('Firestore collection'))
    parser.add_argument('--fixed_windows', required=True, help=('Size of the window in seconds'))
    parser.add_argument('--input_subscription', required=True, help=('Input PubSub subscription'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Required to generate Dataflow template
    pipeline_args.extend([
        '--project=' + known_args.project_id
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    logging.info('Running pipeline ...')
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        ids = (
            p
            | 'read_from_pub_sub' >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription).with_output_types(bytes)
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
            | 'update_database' >> beam.ParDo(FireStoreUpdateFn(
                project_id=known_args.project_id, collection=known_args.collection))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
