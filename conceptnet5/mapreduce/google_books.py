from mrjob.job import MRJob
import os

class GoogleNgramReducer(MRJob):
    def mapper(self, key, line):
        print line
        try:
            rowid, ngram, year, occurrences, pages, books = line.split('\t')
        except ValueError:
            raise ValueError(line[:200])
        if year >= 1960:
            text = ngram.decode('utf-8').lower()
            yield text.encode('utf-8'), int(occurrences)

    def reducer(self, key, values):
        yield key, sum(values)

    def hadoop_input_format(self):
        return 'SequenceFileAsTextInputFormat'

if __name__ == '__main__':
    GoogleNgramReducer.run()
