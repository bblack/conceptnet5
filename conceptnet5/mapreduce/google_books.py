from fabric.api import execute, task, run, cd, sudo, roles, serial
from reductio.tasks import scatter, sort, map, reduce

def reduce_counts(key, values):
    total = 0
    for value_str in values:
        count = value_str.split('\t')[1]
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
def count_bigrams():
    execute('reduce', 'conceptnet5.mapreduce.google_books.reduce_counts', 'gb-bigrams/step0', 'gb-bigrams/counts1')
