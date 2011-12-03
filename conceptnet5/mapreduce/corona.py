# This relies on reductio (github.com/rspeer/reductio) and Fabric.
from reductio.tasks import initialize, initial_scatter, scatter, delete, sort, map, reduce, deploy_worker_config, install_reductio, install_git_package
from fabric.api import execute, task
import itertools
import json
import sys

def load_edges(output):
    for line in open('inputs/edges.json'):
        obj = json.loads(line)
        parts = obj['key'].split(' ')
        if len(parts) != 3:
            raise ValueError(obj['key'])
        type, start, end = parts
        
        if start == '/':
            score = 1
        else:
            score = 0

        value = u"edge\t{type}\t{end}\t{score}".format(type=type,
          end=end, score=score)
        output.write_pair(start, value)

def map_activation(key, value):
    parts = value.split('\t')
    if parts[0] == 'NODE' and key == '/':
        yield '/', u'NODE\t1'
    elif parts[0] == 'edge':
        yield key, value
        endURI = parts[2]
        score = float(parts[3])
        node_value = u"NODE\t{0}".format(score)
        yield endURI, node_value

def reduce_nodes(key, values):
    conjunction = key.startswith('/conjunction')
    sum = 0.
    for value in values:
        parts = value.split('\t')
        val2 = float(parts[-1])
        if conjunction:
            if val2 == 0:
                val2 = 0.000000001
            sum += 1./val2
        else:
            sum += val2
        if parts[0] == 'edge':
            yield key, value
    
    if conjunction:
        sum = 1./sum
    node_value = u"NODE\t{0}".format(sum)
    yield key, node_value

def reduce_edges(key, values):
    yield '/', u'NODE\t1'
    node_score = None
    for value in values:
        parts = value.split('\t')
        val2 = float(parts[-1])
        if parts[0] == 'NODE':
            node_score = val2
        if parts[0] == 'edge' and node_score is not None:
            type = parts[1]
            end = parts[2]
            edge_value = u"edge\t{type}\t{end}\t{node_score}".format(
              type=type, end=end, node_score=node_score
            )
            for key2, value2 in map_activation(key, edge_value):
                yield key2, value2

def reduce_activation(key, values):
    conjunction = key.startswith('/conjunction')
    sum = 0.
    node_score = None
    for value in values:
        parts = value.split('\t')
        val2 = float(parts[-1])
        if node_score is None:
            if parts[0] == 'NODE':
                node_score = val2
        if conjunction:
            if val2 == 0:
                val2 = 0.000000001
            sum += 1./val2
        else:
            sum += val2
        if parts[0] == 'edge':
            type = parts[1]
            end = parts[2]
            edge_value = u"edge\t{type}\t{end}\t{node_score}".format(
              type=type, end=end, node_score=node_score
            )
            yield key, edge_value
    
    if conjunction:
        sum = 1./sum
    node_value = u"NODE\t{0}".format(sum)
    yield key, node_value

@task
def clean():
    # clear out previous data
    execute('delete', 'corona')

@task
def setup():
    # install the appropriate version of ConceptNet on all machines
    execute('install_reductio')
    execute('install_git_package', 'commonsense', 'conceptnet5', 'reductio')

@task
def forward_init():
    sys.stderr.write("init edges\n")
    initialize('conceptnet5.mapreduce.corona.load_edges', 'corona/init',
               'corona/weights_0')

@task
def forward():
    sys.stderr.write("sort\n")
    execute('sort', 'corona/weights_0', 'corona/weights_1')
    sys.stderr.write("map\n")
    execute('map', 'conceptnet5.mapreduce.corona.map_activation', 'corona/weights_1', 'corona/map_1')
    sys.stderr.write("sort-map\n")
    execute('sort', 'corona/map_1', 'corona/map_2')
    execute('scatter', 'corona/map_2', 'corona/map_3')
    execute('delete', 'corona/weights_2')
    execute('delete', 'corona/map_1')
    execute('delete', 'corona/map_2')
    sys.stderr.write("reduce\n")
    execute('reduce', 'conceptnet5.mapreduce.corona.reduce_nodes', 'corona/map_3', 'corona/reduce_1')
    sys.stderr.write("sort-reduce\n")
    execute('sort', 'corona/reduce_1', 'corona/reduce_2')
    execute('scatter', 'corona/reduce_2', 'corona/reduce_3')
    #execute('delete', 'corona/map_3')
    #execute('delete', 'corona/reduce_1')
    #execute('delete', 'corona/reduce_2')

@task
def forward_step2():
    # same except don't use weights_0 as input
    pass
