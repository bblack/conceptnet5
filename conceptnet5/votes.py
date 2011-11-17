"""
Handles votes for assertions.
"""

__author__ = 'Justin Venezuela (jven@mit.edu)'

import json
from conceptnet5.config import get_auth
from conceptnet5.secrets import PASSWORD
from conceptnet5.secrets import USERNAME
from datetime import datetime
from pymongo import Connection

DATABASE_NAME = u'conceptnet-dev'
COLLECTION_NAME = u'votes'

def _get_db(
    server='67.202.5.17', port=27017, username=USERNAME, password=PASSWORD):
  connection = Connection(server, port)
  if DATABASE_NAME not in connection.database_names():
    print 'ERROR: Invalid database name \'%s\'.' % DATABASE_NAME
    return -1
  db = connection[DATABASE_NAME]
  db.authenticate(username, password)
  return db

def _get_or_create_collection(db):
  if COLLECTION_NAME not in db.collection_names():
    db.create_collection(COLLECTION_NAME)
  collection = db[COLLECTION_NAME]
  return collection

def get_vote_collection():
  conceptnet = _get_db()
  vote_collection = _get_or_create_collection(conceptnet)
  return vote_collection

def record_vote(vote_collection, assertion_uri, vote, ip_address):
  vote_document = {
      'assertion_uri':assertion_uri,
      'vote':vote,
      'ip_address':ip_address,
      'datetime':str(datetime.now())
  }
  vote_collection.insert(vote_document)
