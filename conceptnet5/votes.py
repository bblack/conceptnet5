"""
Handles votes for assertions.
"""

__author__ = 'Justin Venezuela (jven@mit.edu)'

import json
from conceptnet5.secrets import PASSWORD
from conceptnet5.secrets import USERNAME
from datetime import datetime
from pymongo import Connection

class VoteDatabase():
  def __init__(self, server='67.202.5.17', port=27017, username=USERNAME,
      password=PASSWORD, database_name=u'conceptnet-dev',
      collection_name=u'votes'):
    self.connection = Connection(server, port)
    if database_name not in self.connection.database_names():
      print 'ERROR: Invalid database name \'%s\'.' % database_name
      return -1
    self.db = self.connection[database_name]
    self.db.authenticate(username, password)
    if collection_name not in self.db.collection_names():
      self.db.create_collection(collection_name)
    self.collection = self.db[collection_name]

  def vote_exists(self, assertion_uri, ip_address):
    return self.collection.find_one(
        {'assertion_uri':assertion_uri, 'ip_address':ip_address}) is not None

  def record_vote(self, assertion_uri, vote, ip_address):
    vote_document = {
        'assertion_uri':assertion_uri,
        'vote':vote,
        'ip_address':ip_address,
        'datetime':str(datetime.now())
    }
    self.collection.insert(vote_document)
