from requests.auth import HTTPDigestAuth
from pygerrit2 import GerritRestAPI, HTTPBasicAuth
import requests
from pprint import pprint
import urllib
import logging

import pathos.multiprocessing as mp

import sys
import traceback
from itertools import repeat
from collections import Counter
import time

def get_diff(url, client):
    """Uses client to return file diff from a given URL."""
    return client.get(url)

def has_votes(change):
    """Returns True if there are any votes for a given change.
    Assumes that change has the keys: positive_reviews_counts and 
    negative_reviews_counts.
    """
    return change.get("positive_reviews_counts", 0) + change.get("negative_reviews_counts", 0) > 0

class GerritScraper(object):
    """A facade to scrap the data from a Gerrit instance."""
    
    def __init__(self, base_url, auth=None, stores=[], sleep_between_pages=0, workers=5):
        """
        Parameters
        ----------
        base_url : str
            The base URL address of the Gerrit instance to scrap from.
        auth : pygerrit2.HTTPBasicAuth, optional
            The data used for authentication (e.g., pygerrit2.HTTPBasicAuth('username', 'password'), 
            None means a guest access (default is None).
        storse : changes stores, optional
            A facade objects used to store retrieved objects  (default is [])
        sleep_between_pages : int, optional
            Pause time between each page being fetched measured in seconds (default is 0)
        workers: int, optional
            The number of worksers used during parallel querying (default is 5)
        """
        self.logger = logging.getLogger('gerrit.scraper.GerritScraper')
        self.base_url = base_url
        self.auth = auth
        self.client =  GerritRestAPI(url=self.base_url, auth = self.auth)
        self.stores = stores
        self.sleep_between_pages = sleep_between_pages
        self.workers = workers
        self.p = None
        self.stats = {'stored': 0, 'processed': 0}


    def _fill_revision_with_files_diffs(self, revision, change):
        """Adds information about changes in files for the revision."""
        files = revision.get("files", {})

        file_ids = files.keys()
        file_ids_urls = [urllib.parse.quote_plus(file_id) for file_id in file_ids]
        diffs_urls = [ "/changes/{}/revisions/{}/files/{}/diff".format(
            change['_number'], revision['_number'], file_id_url) for file_id_url in file_ids_urls]

        file_diffs = []

        if len(file_ids_urls) > 0:
            
            try:
                file_diffs = self.p.starmap(get_diff, zip(diffs_urls, repeat(self.client)))
            except:
                self.logger.info("Exception while processing change {}, revision {}: {}".format(
                    change['_number'], revision['_number'], sys.exc_info()[0]))
                self.logger.error(traceback.format_exc())
            
            if len(file_diffs) > 0 :
                for i, file_id in enumerate(file_ids):
                    file_info = files[file_id]
                    file_info['diff'] = file_diffs[i]
        
        self.logger.info("Processing change {}, revision {}: files {}".format(
                change['_number'], revision['_number'], len(file_diffs)))


    def _process_change(self, change, last_revision_only):
        """Process a ChangeInfo object stores it and returns as it was stored."""
        
        # fill the file changes in revisions
        revisions = change.get('revisions', {})
        if len(revisions) > 0:
            last_revision_number = max([revision['_number'] for revision in revisions.values()])
            change['last_revision'] = last_revision_number
        else:
            change['last_revision'] = None
        if last_revision_only and len(revisions) > 0 :
            revision_id = [key for key, value in revisions.items() \
                            if value['_number'] == last_revision_number][0]
            self._fill_revision_with_files_diffs(revisions[revision_id], change)
        else:
            for revision_id in revisions.keys():
                self._fill_revision_with_files_diffs(revisions[revision_id], change)

        # determine review result
        reviews_counter = Counter([str(x.get('value', '0')) for x in change['labels'].get('Code-Review', {}).get('all', [])])
        change['reviews_counts'] = reviews_counter
        change['positive_reviews_counts'] = sum([v for k, v in reviews_counter.items() if int(k) > 0 ])
        change['negative_reviews_counts'] = sum([v for k, v in reviews_counter.items() if int(k) < 0 ])

        return change
        
    def scrap_changes(self, q="status:open OR status:merged OR status:abandoned&" \
                     "o=ALL_FILES&o=ALL_REVISIONS&o=LABELS&o=DETAILED_LABELS&" \
                     "o=DETAILED_ACCOUNTS&o=MESSAGES", 
                      n=None, pages=None, last_revision_only=True):
        """Is a generator returning changes returned by a given query to Gerrit.

        Parameters
        ----------
        q : str, optional
            A query used as a 'q' parameter of the request for changes.
        n : int, optional
            Limits the number of changes returned by a query, None means not limit (default is None).
        pages : int, optional
            Limits the numbers of pages retrieved from the Gerrit, None means quering as long as 
            there are new pages available (default is None). 
        last_revision_only : bool, optional
            If true, downloads only diffs for files for the last revision (default is True).

        Yields
        ------
        A change information object as it will be stored by the store.
        """
        proc_changes = 1
        self.p = mp.Pool(self.workers)
        self._accounts_cache = {}
        base_query = "/changes/?q={}".format(q)
        if n is not None:
            base_query += "&n={}".format(n)
        page, start = 1, 0
        load_next_page = True
        self.logger.debug(base_query)
        fails = 0
        N = None
        query = base_query
        while load_next_page:
            try:
                query = "{}&start={}".format(base_query, start)
                #if N is not None:
                #    query = "{}&N={}".format(base_query, N)
                changes = self.client.get(query)
                fails = 0
                no_changes = len(changes)
                self.logger.info("Page {}, Changes = {}".format(page, no_changes))

                has_more = changes[no_changes-1].get('_more_changes', False)
                if has_more and (pages is None or page < pages):
                    load_next_page = True
                    start += no_changes
                    N = changes[no_changes-1].get('_sortkey', None)
                    page += 1
                    self.logger.info("Waiting for {} seconds...".format(self.sleep_between_pages))
                    time.sleep(self.sleep_between_pages)
                else:
                    load_next_page = False

                for change in changes:
                    self.stats['processed'] += 1
                    self.logger.info("#{}: Processing change {}".format(proc_changes, change['_number']))
                    processed_change = self._process_change(change, last_revision_only)
                    proc_changes += 1
                    yield processed_change
            except:
                fails += 1
                self.logger.error(traceback.format_exc())
                time_sleep = self.sleep_between_pages + self.sleep_between_pages * fails
                self.logger.info("Waiting for {} seconds because of HTTP error...".format(time_sleep))
                time.sleep(time_sleep)
        self.p.close()
        self.p.join()

            

    def scrap_and_store_changes(self, q="status:open OR status:merged OR status:abandoned&" \
                     "o=ALL_FILES&o=ALL_REVISIONS&o=LABELS&o=DETAILED_LABELS&" \
                     "o=DETAILED_ACCOUNTS&o=MESSAGES", 
                      n=None, pages=None, last_revision_only=True, store_decision_maker=has_votes):
        """Runs the scrap_changes method and stores all of the changes using the store.

        Parameters
        ----------
        q : str, optional
            A query used as a 'q' parameter of the request for changes.
        n : int, optional
            Limits the number of changes returned by a query, None means not limit (default is None).
        pages : int, optional
            Limits the numbers of pages retrieved from the Gerrit, None means quering as long as 
            there are new pages available (default is None). 
        last_revision_only : bool, optional
            If true, downloads only diffs for files for the last revision (default is True).
        store_decision_maker: function, optional
            A function that takes a change as a parameter and returns True if it should be stored
            (default is has_votes).
        """
        try:
            for store in self.stores:
                store.open()
            for change in self.scrap_changes(q, n, pages, last_revision_only):
                if store_decision_maker(change):
                    self.logger.info("Storing change {}".format(change['_number']))
                    for store in self.stores:
                        self.stats['stored'] += store.save_change(change)
                else:
                    self.logger.info("Skipping change {}".format(change['_number']))

        except:
            self.logger.error(traceback.format_exc())
        finally:
            for store in self.stores:
                store.close()
            
            

