import sys
import os
from mrjob.job import MRJob
import simplejson

def parallel(a, b):
    if a <= 0 or b <= 0:
        return 0.
    return float(a*b) / (a + b)

class CORONAForwardStep(MRJob):
    PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def __init__(self, *args, **kwargs):
        self.current_node = None
        self.current_value = None
        MRJob.__init__(self, *args, **kwargs)

    def map_json(self, key, value):
        # first step: the actual key is None
        value = simplejson.loads(value)
        startURI = value['start']
        endURI = value['end']
        weight = value.get('weight', 1.0)
        type = value['type']
        if not ('|' in startURI or '|' in endURI):
            edge_value = u"%(type)s|%(end)s|%(weight)s" % dict(
                type=type, end=endURI, weight=weight
            )
            yield startURI+"|edge", edge_value
            score = 0
            if key == '/':
                score = 1.0
            node_value = str(score*weight)
            yield startURI+"|NODE", '0'
            yield endURI+"|NODE", node_value

    def reduce_activation(self, key, values):
        key, objtype = key.split('|')

        conjunction = key.startswith('/conjunction')
        if objtype == 'NODE':
            if key == '/':
                sum = 1.
            else:
                sum = None
                for value in values:
                    val2 = float(value)
                    if sum is None:
                        sum = val2
                    else:
                        if conjunction:
                            sum = parallel(sum, val2)
                        else:
                            sum += val2
            self.current_node = key
            self.current_value = sum

        elif objtype == 'edge':
            if key == '/':
                self.current_node = '/'
                self.current_value = 1.
            for value in values:
                parts = value.split('|')
                if len(parts) < 3:
                    raise ValueError(str(values))
                if self.current_node != key:
                    # Set a flag value for when this happens (because it
                    # shouldn't)
                    self.current_node = key
                    self.current_value = 0.0123
                type = parts[0]
                end = parts[1]
                weight = float(parts[2])
                node_score = self.current_value * weight
                edge_value = u"%(type)s|%(end)s|%(weight)s" % dict(
                  type=type, end=end, weight=weight
                )
                yield key+"|edge", edge_value
                node_value = str(node_score)
                yield end+"|NODE", node_value
        else:
            raise ValueError(objtype)
    
    def reduce_nop(self, key, values):
        yield key, list(values)

    def reduce_final(self, key, values):
        key, objtype = key.split('|')
        conjunction = key.startswith('/conjunction')
        sum = None

        for value in values:
            parts = value.split('|')
            val2 = float(parts[-1])
            if objtype == 'NODE':
                if sum is None:
                    sum = val2
                else:
                    if conjunction:
                        sum = parallel(sum, val2)
                    else:
                        sum += val2
        if sum:
            output = {'key': key, 'score': sum}
            yield key, output

    def steps(self):
        return [self.mr(self.map_json, self.reduce_activation),
                self.mr(None, self.reduce_activation),
                self.mr(None, self.reduce_activation),
                self.mr(None, self.reduce_activation),
                #self.mr(None, self.reduce_nop),]
                self.mr(None, self.reduce_final)]

if __name__ == '__main__':
    CORONAForwardStep.run()
