import logging

import apache_beam as beam

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
        logging.info('Init FireStoreUpdateFn ...')
        self.project_id = project_id
        self.collection = collection

    def start_bundle(self):
        logging.info('Starting FireStore Client ...')
        self._db = firestore.Client(project=self.project_id)

    def process(self, item):
        id = item['id']
        clicks = item['clicks']

        doc_ref = self._db.collection(self.collection).document(id)
        doc_ref.update({"clicks": firestore.Increment(clicks)})

        logging.info(f'Incremented on FireStore: {item}')
