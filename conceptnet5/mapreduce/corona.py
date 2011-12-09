import sys
import os
from mrjob.job import MRJob

class CORONAForwardStep(MRJob):
    def map_activation(self, key, value):
        if key is None:
            key, value = value.split('\t', 1)
        try:
            parts = value.split('\t')
            if parts[0] == 'NODE':
                yield key, value
            elif parts[0] == 'edge':
                yield key, value
                endURI = parts[2]
                score = float(parts[3])
                if key == '/':
                    score = 1.0
                if score > 0:
                    node_value = u"NODE\t%s" % score
                    yield endURI, node_value
            else:
                raise ValueError("Unknown thing: (%r, %r)" % (key, value))
        except ValueError:
            print "ValueError for (%r, %r)" % (key, value)

    def reduce_activation(self, key, values):
        conjunction = key.startswith('/conjunction')
        sum = 0.
        node_score = 0.
        if key == '/':
            node_score = 1.
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
                edge_value = u"edge\t%(type)s\t%(end)s\t%(node_score)s" % dict(
                  type=type, end=end, node_score=node_score
                )
                yield key, edge_value
        
        if conjunction:
            sum = 1./sum
            if sum < 1e-8:
                sum = 0
        node_value = u"NODE\t%s" % sum
        yield key, node_value

    def steps(self):
        return [self.mr(self.map_activation, self.reduce_activation),
                self.mr(self.map_activation, self.reduce_activation),
                self.mr(self.map_activation, self.reduce_activation)]

if __name__ == '__main__':
    CORONAForwardStep.run()
