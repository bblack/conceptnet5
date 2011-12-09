import sys
import os
from mrjob.job import MRJob
import simplejson

def parallel(a, b):
    if a <= 0 or b <= 0:
        return 0.
    return float(a*b) / (a + b)

class CORONAForwardStep(MRJob):
    def map_json(self, key, value):
        # first step: the actual key is None
        value = simplejson.loads(value)
        startURI = value['start']
        endURI = value['end']
        weight = value['weight']
        type = value['type']
        if not ('\t' in startURI or '\t' in endURI):
            edge_value = u"edge\t%(type)s\t%(end)s\t%(weight)s" % dict(
                type=type, end=endURI, weight=weight
            )
            yield startURI, edge_value
            score = 0
            if key == '/':
                score = 1.0
            node_value = u"NODE\t%s" % (score*weight)
            yield endURI, node_value

    def reduce_activation(self, key, values):
        conjunction = key.startswith('/conjunction')
        sum = None
        if key == '/':
            sum = 1.
        for value in values:
            parts = value.split('\t')
            val2 = float(parts[-1])
            if parts[0] == 'NODE' and key != '/':
                if sum == None:
                    sum = val2
                else:
                    if conjunction:
                        sum = parallel(sum, val2)
                    else:
                        sum += val2
                        
            if parts[0] == 'edge':
                type = parts[1]
                end = parts[2]
                weight = float(parts[3])
                node_score = sum*weight
                edge_value = u"edge\t%(type)s\t%(end)s\t%(weight)s" % dict(
                  type=type, end=end, weight=weight
                )
                yield key, edge_value
                node_value = u"NODE\t%s" % node_score
                yield end, node_value

    def reduce_nop(self, key, values):
        for value in values:
            yield key, value

    def steps(self):
        return [self.mr(self.map_json, self.reduce_activation),
                self.mr(None, self.reduce_activation),
                self.mr(None, self.reduce_activation),
                self.mr(None, self.reduce_activation)]

if __name__ == '__main__':
    CORONAForwardStep.run()
