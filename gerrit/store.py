from pprint import pprint
import logging

import sys
import traceback
from itertools import repeat
from collections import Counter
import time

import json
from pymongo import MongoClient
from pymongo.son_manipulator import SONManipulator


class KeyAndIntTransform(SONManipulator):
    """Transforms keys going to a database and restores them when queried back.
    It replaces given strings in keys and converts all integer keys to strings.
    """

    def __init__(self, replace, replacement):
        """
        Parameters
        ----------
        replace: str
            String to be replaced in the keys.
        replacement: str
            String to replace every replace substring in keys.
        """
        self.replace = replace
        self.replacement = replacement

    def transform_key(self, key):
        """Transform the key for saving to a database.
        
        Parameters
        ----------
        key: str
            A key to be transformed while storing to a database.
        """
        return str(key).replace(self.replace, self.replacement)

    def revert_key(self, key):
        """Restore a transformed key whyle returning the object from a database.
        Integers keys are not restored to their original form, they remain as strings.

        Parameters
        ----------
        key: str
            A key to be transformed back to the original form.
        """
        return str(key).replace(self.replacement, self.replace)


    def transform_incoming(self, son, collection):
        """Recursively replace all keys that need to be transformed."""

        new_son = {}
        for (key, value) in son.items():
            if not isinstance(key, str) or self.replace in key:
                new_son[self.transform_key(key)] = son[key]
            else:
                new_son[key] = son[key]
        
        son = new_son

        for (key, value) in son.items():
            if isinstance(value, dict):
                son[key] = self.transform_incoming(value, collection)

        return son

    def transform_outgoing(self, son, collection):
        """Recursively restore all previously transformed keys 
        (the keys being integers will be returned as strings)."""
        new_son = {}
        for (key, value) in son.items():
            if not isinstance(key, str) or self.replacement in key:
                new_son[self.revert_key(key)] = son[key]
            else:
                new_son[key] = son[key]
        
        son = new_son

        for (key, value) in son.items():
            if isinstance(value, dict):
                son[key] = self.transform_outgoing(value, collection)

        return son

class MongoDBStore(object):
    """Changes store writing a MongoDB database."""

    def __init__(self, mongo_url='localhost', port=27017, 
                db_name='gerrit_db', collection_name='reviews',
                clear_before=True, skip_existing=False):
        """
        Parameters
        ----------
        mongo_url: str, optional
            URL to the MongoDB instance (default is localhost).
        port: int, optional
            Port on which the MongoDB instance is available (default is 27017).
        db_name: str, optional
            The name of the database to which the data will be stored (default is gerrit_db).
        collection_name: str, optional
            The name of the collection that will store the changes (default is reviews).
        clear_before: bool, optional
            If True the collection will be purged on open (default is True).
        """
        self.mongo_url = mongo_url
        self.port = port
        self.collection_name = collection_name
        self.db_name = db_name
        self.clear_before = clear_before
        self.skip_existing = skip_existing
        self.logger = logging.getLogger('gerrit.store.MongoDBStore')
        
    def open(self):
        """Opens connection to the database and purges the collection if requested."""
        self.client = MongoClient(self.mongo_url, self.port)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        if self.clear_before:
            self.collection.delete_many({})
        self.db.add_son_manipulator(KeyAndIntTransform(".", "__dot__"))

    def save_change(self, change):
        """Saves a Gerrit change to the MongoDB.

        Parameters
        ----------
        change: change from Gerrit
            A change from Gerrit that is supposed to be saved.

        Returns
        ---------
        Number of changes stored.
        """
        # remove _more_changes
        if '_more_changes' in change.keys():
            change.pop('_more_changes')

        document_replaced = None
        if not self.clear_before or self.skip_existing:
            document_replaced = self.collection.find_one(
                {"_number":{"$eq":change['_number']}})
            if document_replaced is not None:
                change['_id'] = document_replaced['_id']

        if self.skip_existing and document_replaced is not None:
            self.logger.info("Skipping {}, it already exists...".format(change['_number']))
        else:
            try:
                self.collection.save(change)
                return 1
            except:
                self.logger.error(traceback.format_exc())
        return 0

    def close(self):
        """Closes the connection to the database."""
        self.client.close()

class JSONFileStore(object):
    """Changes store writing a JSON file."""

    def __init__(self, file_path):
        """
        Parameters
        ----------
        file_path : str
            Path to the json output file.
        """
        self.file_path = file_path
        self._records_stored = 0
        self._file = None

    def open(self):
        """Ereases the output file and then opens it for writing."""
        # clear the exisiting file
        open(self.file_path, 'w').close()
        self.file = open(self.file_path, 'a', encoding="utf-8")
        self.file.write("[")

    def save_change(self, change):
        """Saves a Gerrit change to a file.

        Parameters
        ----------
        change: change from Gerrit
            A change from Gerrit that is supposed to be saved.
        
        Returns
        ---------
        Number of changes stored.
        """
        # remove _more_changes
        if '_more_changes' in change.keys():
            change.pop('_more_changes')

        change_json = json.dumps(change, ensure_ascii=False, indent=4)
        if self._records_stored > 0:
            self.file.write(",\n")
        self.file.write(change_json)
        self._records_stored += 1
        return 1

    def close(self):
        """Closes the output file."""
        self.file.write("]\n")
        self.file.close()
