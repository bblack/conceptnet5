from fabric.api import execute, task, run, cd, sudo, roles, serial, env
from reductio.tasks import scatter, sort, map, reduce, install_git_package, delete, install_reductio
from reductio import config
import os

config.HOMEDIR = os.path.expanduser('~/.reductio/master')

def reduce_counts_ngram(key, values):
    total = 0
    for value_str in values:
        count = int(value_str.split('\t')[1])
        total += count
    if ' ' in key:
        for word in key.split(' '):
            yield word, total
    yield key, total

def reduce_counts(key, values):
    total = 0
    for value_str in values:
        count = int(value_str.split('\t')[1])
        total += count
    yield key, total

@task
@roles('workers')
@serial
def prereqs():
    run('mkdir -p /data/reductio/gb-bigrams/step0')
    sudo('aptitude install unzip')

@task
@roles('workers')
def unzip_files():
    with cd('/data/reductio/google-books'):  # make more general
        run('for i in *.zip; do unzip $i; mv ${i%.zip} /data/reductio/gb-bigrams/step0/; done')

@task
def setup():
    execute('install_reductio')
    execute('install_git_package', 'commonsense', 'conceptnet5', 'reductio')
    execute('delete', 'gb-bigrams/counts1')

@task
def count_bigrams():
    execute('reduce', 'conceptnet5.mapreduce.google_books.reduce_counts_ngram', 'gb-bigrams/step0', 'gb-bigrams/counts1')
    execute('scatter', 'gb-bigrams/counts1', 'gb-bigrams/counts2')
    execute('sort', 'gb-bigrams/counts2', 'gb-bigrams/counts3')
    execute('reduce', 'conceptnet5.mapreduce.google_books.reduce_counts_ngram', 'gb-bigrams/counts3', 'gb-bigrams/counts4')
