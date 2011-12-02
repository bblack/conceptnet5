# This relies on reductio (github.com/rspeer/reductio) and Fabric.
from reductio.tasks import *
from fabric.api import *

def load_edges(output):
    for line in open('inputs/edges.json'):
        obj = json.loads(line)
        type, start, end = obj['key'].split()
        
        if start == '/':
            score = 1
        else:
            score = 0

        value = "edge\t{type}\t{end}\t{score}".format(locals())
        output.write_pair(start, value)

def load_nodes(output):
    for line in open('inputs/nodes.json'):
        obj = json.loads(line)
        uri = obj['uri']
        if uri == '/':
            score = 1
        else:
            score = 0
        value = "NODE\t{score}".format(score=score)
        output.write_pair(uri, value)

def map_activation(key, value):
    parts = value.split('\t')
    if parts[0] == 'NODE' and key == '/':
        yield '/', 1
    elif parts[0] == 'edge':
        yield key, value
        endURI = parts[2]
        score = float(parts[3])
        node_value = "NODE\t{score}".format(score=score)
        yield endURI, node_value

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
            edge_value = "edge\t{type}\t{end}\t{node_score}".format(locals())
            yield key, edge_value
    
    if conjunction:
        sum = 1./sum
    node_value = "NODE\t{score}".format(score=score)
    yield key, node_value

@task
def setup():
    # install the appropriate version of ConceptNet on all machines
    execute('install_git_package', 'commonsense', 'conceptnet', 'reductio')

    # clear out previous data
    execute('delete', 'corona')

@task
def forward_start():
    initialize('conceptnet5.corona.fabfile.load_nodes', 'corona/init',
               'corona/weights_0')
    initialize('conceptnet5.corona.fabfile.load_edges', 'corona/init',
               'corona/weights_0')
    execute('scatter', 'corona/weights_0', 'corona/weights_1')
    execute('sort', 'corona/weights_1', 'corona/weights_2')

@task
def forward_step():
    pass
    # start with 1.0 activation at the root
    # read in weights for nodes and edges
