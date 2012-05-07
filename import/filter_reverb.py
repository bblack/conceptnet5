"""
Functions to parse the ReVerb dataset, 
			 constrain/filter the statements, and 
			 output assertions to a file.
"""
from conceptnet5.graph import JSONWriterGraph
from conceptnet5.english_nlp import normalize, normalize_topic
from urlparse import urlparse
import urllib
import codecs
import nltk
import os
import re
import json

###################################################################
#
# Create or get initial nodes representing reverb, wikipedia, & rules
#
#
###################################################################

GRAPH = JSONWriterGraph('json_data/reverb')
reverb = GRAPH.get_or_create_node(u'/source/rule/reverb')
GRAPH.justify('/', reverb, 0.5)
reverb_object = GRAPH.get_or_create_node(u'/source/rule/extract_reverb_objects')
reverb_triple = GRAPH.get_or_create_node(u'/source/rule/reverb_present_tense_triples')
wikipedia = GRAPH.get_or_create_node(u'/source/web/en.wikipedia.org')
GRAPH.justify('/', reverb_object, 0.2)
GRAPH.justify('/', reverb_triple, 0.5)
GRAPH.justify('/', wikipedia)

###################################################################
#
# Globals
#
#
###################################################################

PRONOUN_TAGS = ('PRP', 'PRP$', 'WP', 'WP$')
PROPER_NOUN_TAGS = ('NNP, NNPS')
WEIGHT_THRESH = 0.6
NEGATIVES = ['not', "n't", 'never', 'rarely']
BE = ['is', 'are', 'was', 'were', 'be']
TYPE_WORDS = ('type', 'kind', 'sort', 'variety', 'one')
# Wikipedia sources.
# we persist this info in the filenames of the articles we extract from Wikipedia
ARTICLE_NAME = re.compile(r'wiki_(.+).txt')


###################################################################
#
# Reverb output parsing and filtering classes
#
#
###################################################################	
class ReverbLine:

	"""Represents a parsed line from a reverb output file
	"""

	def __init__(self, data):
		self.filename, self.sent_num, self.arg1, self.rel, self.arg2, \
   	 	self.arg1_start, self.arg1_end, self.rel_start, self.rel_end, self.arg2_start, self.arg2_end, \
    		self.confidence, self.surfaceText, self.pos_tags, self.chunk_tags, \
    		self.nor_arg1, self.nor_rel, self.nor_arg2, self.tokens, self.tags, self.sources = data

	
	@staticmethod
	def index_of_be(tokens):
		# returns the index of the first 'be' (or one of its forms) in the sentence
		for token in tokens:
		    if token in BE:
		        return tokens.index(token)
		return -1

	@staticmethod
	def index_of_verbs(tags):
		# returns a list of indices of the verbs in the sentence
		index = []
		for tag in tags:
		    if tag.startswith('V'):
		        index.append(tags.index(tag))
		return index

	@staticmethod
	def contain_single_be(tokens, tags):
		# returns the part of speech tag of the 'be' or 'been' in the sentence
		# if the sentence only has a single 'be' or 'been'
		verbs = filter(lambda x: x.startswith('V'), tags)
		if len(verbs) == 1 and tokens[tags.index(verbs[0])] in BE:
		    return tags.index(verbs[0])
		elif len(verbs) == 2 and tokens[tags.index(verbs[1])] == 'been':
		    return tags.index(verbs[1])
		return -1

	@staticmethod
	def parse_line(line):
		"""Parses a line from a reverb output file or returns None if it is malformed
		"""

		parts = line.split('\t')
		if len(parts) < 18:
		    return None

		filename, sent_num, arg1, rel, arg2, \
		arg1_start, arg1_end, rel_start, rel_end, arg2_start, arg2_end, \
		confidence, surfaceText, pos_tags, chunk_tags, \
		nor_arg1, nor_rel, nor_arg2 = parts


		match = ARTICLE_NAME.match(filename.split('/')[-1])
		if not match:
		    	return None

		sources = [match.group(1)]

		arg1_start = int(arg1_start)
		arg1_end = int(arg1_end)
		arg2_start = int(arg2_start)
		arg2_end = int(arg2_end)
		confidence = float(confidence)

		tokens  = re.split('\s', surfaceText)
		tags = re.split('\s', pos_tags)
		assert(len(tokens) == len(tags))	

		data = filename, sent_num, arg1, rel, arg2, \
		arg1_start, arg1_end, rel_start, rel_end, arg2_start, arg2_end, \
		confidence, surfaceText, pos_tags, chunk_tags, \
		nor_arg1, nor_rel, nor_arg2, tokens, tags, sources

		result = ReverbLine(data)

		return result
    
class ReverbFilter:
    """contains functions that are used to constrain/filter Reverb output  
	"""
    @staticmethod
    def is_low_confidence(reverb_line):
		# this extraction is assigned a low confidence by the Reverb extractor
		return reverb_line.confidence < WEIGHT_THRESH
    @staticmethod
    def is_numeric(reverb_line):
		# one of the arguments is a number
		return reverb_line.arg1[0].isdigit() or reverb_line.arg2[0].isdigit()
    @staticmethod
    def has_pronoun_arg(reverb_line):
		#one of the arguments contains a pronoun
        for pronoun in PRONOUN_TAGS:
            if not pronoun in reverb_line.tags: 
                continue
            idx = reverb_line.tags.index(pronoun)
            return (reverb_line.arg1_start <= idx <= reverb_line.arg1_end) or \
					(reverb_line.arg2_start <= idx <= reverb_line.arg2_end)
    @staticmethod
    def not_triple(reverb_line):
		return reverb_line.arg2.strip() == ""

    @staticmethod 
    def contains_article_name(reverb_line): 
        name = reverb_line.sources[0]
        return name in reverb_line.arg1 or name in reverb_line.arg2
    
    @staticmethod
    def coordinates_proper_nouns(reverb_line):
        in_arg1 = False
        in_arg2 = False

        for pn in PROPER_NOUN_TAGS:
            if not pn in reverb_line.tags: 
                continue

            idx = reverb_line.tags.index(pn)
            if reverb_line.arg1_start <= idx <= reverb_line.arg1_end: 
                in_arg1 = True
            elif reverb_line.arg2_start <= idx <= reverb_line.arg2_end: 
                in_arg2 = True
	
        return in_arg1 or in_arg2	

###################################################################
#
# Utility functions
#
#
################################################################### 

def bracket_concepts(text, start1, end1, start2, end2):
    words = re.split('\s', text.strip())
    words.insert(start1, "[[")
    words.insert(end1 + 1, "]]")
    words.insert(start2 + 2, "[[")
    words.insert(end2 + 3, "]]")
    return ' '.join(words)
        
def output_reverb_data(reverb_line, rules):
    """
    Output the relevant reverb data in json. This includes the
    text of the two related terms, the original text in the sentence, the
    source page, and the weight, and a list of programmatic rules that 
    were applied to produce this statement". Rule names URIs that look like
    /source/rule/reverb/make_up_a_name_here.
    """
    result = {"rel": reverb_line.rel,
            "arg1": reverb_line.arg1,
            "arg2": reverb_line.arg2,
            "weight" : reverb_line.confidence,
            "sources" : reverb_line.sources,
            "surfaceText" :  bracket_concepts(reverb_line.surfaceText,
                                              reverb_line.arg1_start, 
											  reverb_line.arg1_end,
                                              reverb_line.arg2_start, 
											  reverb_line.arg2_end),
            "rules": rules}

    print json.dumps(result)
    print '\n'
    return result

def normalize_rel(text):
    parts = normalize(text).split()
    if len(parts) >= 2 and parts[1] == 'be' and parts[0] in ('be', 'have'):
        parts = parts[1:]
    parts = [p for p in parts if p != 'also']
    return ' '.join(parts)

def probably_present_tense(text):
    return text in ('is', 'are') or normalize(text) == text

def remove_tags(tokens, tags, target):
    index = 0
    iterations = 0
    max_itr = len(tokens)
    while target in tags and iterations < max_itr:
        index_rb = tags.index(target)
        iterations += 1
        if index > 0:
			if tokens[index] in NEGATIVES:
				tags[index] = 'NEG'
				continue
	
			tokens.remove(tokens[index])
			tags.remove(tags[index])

    return tokens, tags

def untokenize(token_list):
    return ' '.join(token_list)


###################################################################
#
# Functions to add assertions to GRAPH
#
#
################################################################### 

def output_raw(reverb_line):
    frame = u"{1} %s {2}" % (reverb_line.rel)

    raw = GRAPH.get_or_create_assertion(
        GRAPH.get_or_create_frame('en', frame),
        [GRAPH.get_or_create_concept('en', reverb_line.arg1),
         GRAPH.get_or_create_concept('en', reverb_line.arg2)],
        {'dataset': 'reverb/en', 'license': 'CC-By-SA',
         'normalized': False,
         'sources': '|'.join(reverb_line.sources)}
    )
    
    conjunction = GRAPH.get_or_create_conjunction([wikipedia, reverb])
    GRAPH.justify(conjunction, raw, weight=reverb_line.confidence)

    for source in reverb_line.sources:
        # Put in context with Wikipedia articles.
        topic = source #topic is the same as the article name
        context = GRAPH.get_or_create_concept('en', topic)
        GRAPH.add_context(raw, context)

    rules = [reverb]
    output_reverb_data(reverb_line, rules) 
    return raw


def output_triple(reverb_line, raw):
    arg1 = normalize(reverb_line.arg1).strip()
    arg2 = normalize(reverb_line.arg2).strip()
    relation = normalize_rel(reverb_line.rel).strip()

    found_relation = True
    if relation == 'be for' or relation == 'be used for' :
        relation = 'UsedFor'
    elif relation == 'be not':
        relation = 'IsNot'
    elif relation == 'be part of':
        relation = 'PartOf'
    elif relation == 'be similar to':
        relation = 'SimilarTo'
    elif relation.startswith('be ') and relation.endswith(' of') and relation[3:-3] in TYPE_WORDS:
        relation = 'IsA'
    else:
        found_relation = False

    if found_relation:
        rel_node = GRAPH.get_or_create_relation(relation)
    else:
        rel_node = GRAPH.get_or_create_concept('en', relation)
   	print '%s(%s, %s)' % (relation, arg1, arg2),

    assertion = GRAPH.get_or_create_assertion(
        rel_node,
        [GRAPH.get_or_create_concept('en', arg1),
         GRAPH.get_or_create_concept('en', arg2)],
        {'dataset': 'reverb/en', 'license': 'CC-By-SA',
         'normalized': True}
    )

    GRAPH.derive_normalized(raw, assertion, weight=reverb_line.confidence)

    conjunction = GRAPH.get_or_create_conjunction([raw, reverb_triple])
    GRAPH.justify(conjunction, assertion)

    for source in reverb_line.sources:
        # Put in context with Wikipedia articles.
        topic = source
        context = GRAPH.get_or_create_concept('en', topic)
        context_normal = GRAPH.get_or_create_concept('en', *normalize_topic(topic))
        GRAPH.add_context(assertion, context_normal)
        GRAPH.get_or_create_edge('normalized', context, context_normal)
       	print "in", context_normal

	rules = [reverb_triple]
	reverb_line.arg1 = arg1
	reverb_line.arg2 = arg2	
	reverb_line.rel = relation
    output_reverb_data(reverb_line, rules) 

    return assertion

def output_sentence(relation, arg1, arg2, raw, reverb_line):
	
	arg1 = normalize(arg1).strip()
	arg2 = normalize(arg2).strip()
	assertion = None
	
	print '%s(%s, %s)' % (relation, arg1, arg2)
	assertion = GRAPH.get_or_create_assertion(
        '/relation/'+relation,
        [GRAPH.get_or_create_concept('en', arg1),
        GRAPH.get_or_create_concept('en', arg2)],
        {'dataset': 'reverb/en', 'license': 'CC-By-SA',
        'normalized': True}
        )
	
	conjunction = GRAPH.get_or_create_conjunction([raw, reverb_object])
	GRAPH.justify(conjunction, assertion, weight=reverb_line.confidence)
	
	for source in reverb_line.sources:
		# Put in context with Wikipedia articles
		topic = source
		context = GRAPH.get_or_create_concept('en', *normalize_topic(topic))
		GRAPH.add_context(assertion, context)
	
	rules = [reverb_object]

	reverb_line.arg1 = arg1
	reverb_line.arg2 = arg2	
	reverb_line.rel = relation
	output_reverb_data(reverb_line, rules) 
	
	return assertion

def process_reverb_object(reverb_line, raw):
	# isolate the tokens and tags in the sentence that are a part of this assertion
    tokens = reverb_line.tokens[reverb_line.arg1_start : reverb_line.arg2_end]
    tags = reverb_line.tags[reverb_line.arg1_start : reverb_line.arg2_end]

	# remove adverbs, modals, lowercase the tokens
    tokens, tags = remove_tags(tokens, tags, 'RB')	
    tokens, tags = remove_tags(tokens, tags, 'MD')	
    tokens = map(lambda x: x.lower(), tokens)	

	# no verbs, won't be a useful assertion
    index_verbs = ReverbLine.index_of_verbs(tags)
    if len(index_verbs) == 0: return

	# the 'be' or 'been' is at the end, not useful
    index_be = ReverbLine.contain_single_be(tokens, tags)
    if index_be == len(tokens) - 1: return
    index_prep = 0
    if 'IN' in tags:
		# the preposition is after the first verb in the triple
        if tags.index('IN') > index_verbs[0]:
                index_prep = tags.index('IN')
    if 'TO' in tags:
        index_to = tags.index('TO')
		# the 'to' is before the preposition or
		# the 'to' is after the first verb in the sentence, ie. "He has been to the bar"
        if ((index_to < index_prep and index_prep > 0) or (index_prep == 0)) and (index_to > index_verbs[0]):
			    index_prep = tags.index('TO')
    
    if index_be > 0:
        if tokens[index_be] == 'been':
            arg1 = untokenize(tokens[:index_be-1])
        else:
            arg1 = untokenize(tokens[:index_be]) 
			
        next_tag = tags[index_be+1]

        if next_tag == 'DT': # IsA relation
            if index_prep == 0:
                arg2 = untokenize(tokens[index_be+2:])
                output_sentence('IsA', arg1, arg2, raw, reverb_line)
            else:
                if tokens[index_prep] == 'of' and \
                    tokens[index_prep-1] in TYPE_WORDS:
                    # 'a kind of' frame
                    arg2 = untokenize(tokens[index_prep+1:])
                    output_sentence('IsA', arg1, arg2, raw, reverb_line)

                elif tokens[index_prep] == 'of' and \
                    tokens[index_prep-1] == 'part':
                    # 'a part of' frame
                    arg2 = untokenize(tokens[index_prep+1:])
                    output_sentence('PartOf', arg1, arg2, raw, reverb_line)
        else:
            if index_prep == 0:
                arg2 = untokenize(tokens[index_be+1:])
                output_sentence('HasProperty', arg1, arg2, raw, reverb_line)
    else:
        index_be = ReverbLine.index_of_be(tokens)
        if index_be == len(tokens) - 1: return

        if (index_be > 0) and \
            (index_verbs[0] == index_be or \
            len(index_verbs) > 1): 
            if tokens[index_be] == 'been':
                arg1 = untokenize(tokens[:index_be-1])
            else:
                arg1 = untokenize(tokens[:index_be])

            if tags[index_be+1] == 'VBG': 
                relation = 'SubjectOf'
            else: 
                relation = 'DirectObjectOf'

            if index_prep == 0:
                arg2 = untokenize(tokens[index_be+1:])
                output_sentence(relation, arg1, arg2, raw, reverb_line)
	
###################################################################
#
# Process reverb output files. Choose what extractions are included
#
#
################################################################### 

def handle_line(line):
    rline = ReverbLine.parse_line(line)
    if not rline: return    

	# do all filtering of statements here
    if ReverbFilter.is_low_confidence(rline) or ReverbFilter.is_numeric(rline) or \
        ReverbFilter.has_pronoun_arg(rline) or ReverbFilter.not_triple(rline) or \
        not ReverbFilter.contains_article_name(rline):
		return

	#process all the statements that have passed our filters
    raw = output_raw(rline) #output assertion without normalizing the arguments or relation

	#if present tense, output a normalized triple
    if probably_present_tense(rline.rel.split()[0]):
        triple = output_triple(rline, raw)
		
	# ouput assertion based on 'be' or 'been'
    process_reverb_object(rline, raw)

def handle_file(filename):
    import traceback
    for line in codecs.open(filename, encoding='utf-8', errors='replace'):
        line = line.strip()
        if line:
            handle_line(line)

if __name__ == '__main__':
    import sys, locale
    if sys.stdout.encoding is None:
       (lang, enc) = locale.getdefaultlocale()
       if enc is not None:
           (e, d, sr, sw) = codecs.lookup(enc)
	   # sw will encode Unicode data to the locale-specific character set.
	   sys.stdout = sw(sys.stdout)
    for filename in sys.argv[1:]:
        handle_file(filename)
