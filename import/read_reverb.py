"""
Parse the ReVerb dataset and put assertions to ConceptNet 5
"""
from conceptnet5.graph import JSONWriterGraph
from conceptnet5.english_nlp import normalize, normalize_topic, tokenize, untokenize
from urlparse import urlparse
import urllib
import codecs
import nltk
import os
import re

GRAPH = JSONWriterGraph('json_data/reverb')

reverb = GRAPH.get_or_create_node(u'/source/rule/reverb')
GRAPH.justify('/', reverb, 0.5)
reverb_object = GRAPH.get_or_create_node(u'/source/rule/extract_reverb_objects')
#reverb_prep = GRAPH.get_or_create_node(u'/source/rule/extract_reverb_prepositions')
reverb_triple = GRAPH.get_or_create_node(u'/source/rule/reverb_present_tense_triples')
wikipedia = GRAPH.get_or_create_node(u'/source/web/en.wikipedia.org')
GRAPH.justify('/', reverb_object, 0.2)
GRAPH.justify('/', reverb_triple, 0.5)
GRAPH.justify('/', wikipedia)

TYPE_WORDS = ('type', 'kind', 'sort', 'variety', 'one')

# Search for non-namespaced Wikipedia sources.
#don't need this anymore as Reverb1.3 does not include this information
#WIKIPEDIA_SOURCE = re.compile(r'(http://en.wikipedia.org/wiki/([^:]|:_)+)(\||$)')
#instead, we persist this info in the filenames of the articles we extract from Wikipedia
ARTICLE_NAME = re.compile(r'wiki_(.+).txt')
def normalize_rel(text):
    parts = normalize(text).split()
    if len(parts) >= 2 and parts[1] == 'be' and parts[0] in ('be', 'have'):
        parts = parts[1:]
    parts = [p for p in parts if p != 'also']
    return ' '.join(parts)

def probably_present_tense(text):
    return text in ('is', 'are') or normalize(text) == text

def contain_single_be(tokens, tags):
    be = ['is', 'are', 'was', 'were', 'be']
    verbs = filter(lambda x: x.startswith('V'), tags)
    if len(verbs) == 1 and tokens[tags.index(verbs[0])] in be:
        return tags.index(verbs[0])
    elif len(verbs) == 2 and tokens[tags.index(verbs[1])] == 'been':
        return tags.index(verbs[1])
    return -1

def index_of_tag(tags, target):
    if target in tags:
        return tags.index(target)
    return -1

def index_of_be(tokens):
    be = ['is', 'are', 'was', 'were', 'be', 'been']
    for token in tokens:
        if token in be:
            return tokens.index(token)
    return -1

def index_of_verbs(tags):
    index = []
    for tag in tags:
        if tag.startswith('V'):
            index.append(tags.index(tag))
    return index

NEGATIVES = ['not', "n't", 'never', 'rarely']

def remove_tags(tokens, tags, target):
    index_rb = 0
    iterations = 0
    while target in tags and iterations < 10:
        iterations += 1
        index_rb = tags.index(target)
        if index_rb > 0:
            if tokens[index_rb] not in NEGATIVES:
                tokens.remove(tokens[index_rb])
                tags.remove(tags[index_rb])
            else:
                tags[index_rb] = 'NEG'
    return tokens, tags

#def get_domain_names(urls):
#    parsed_urls = map(lambda x: urlparse(x), urls)
#    domain_names = map(lambda x: x.netloc, parsed_urls)
#    return domain_names

def output_triple(arg1, arg2, relation, raw, sources=[]):
    arg1 = normalize(arg1).strip()
    arg2 = normalize(arg2).strip()
    relation = normalize_rel(relation).strip()
    found_relation = True
    if relation == 'be for':
        relation = 'UsedFor'
    elif relation == 'be used for':
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
    print '%s(%s, %s)' % \
        (relation, arg1, arg2),

    assertion = GRAPH.get_or_create_assertion(
        rel_node,
        [GRAPH.get_or_create_concept('en', arg1),
         GRAPH.get_or_create_concept('en', arg2)],
        {'dataset': 'reverb/en', 'license': 'CC-By-SA',
         'normalized': True}
    )
    GRAPH.derive_normalized(raw, assertion)
    
    conjunction = GRAPH.get_or_create_conjunction([raw, reverb_triple])
    GRAPH.justify(conjunction, assertion)
    for source in sources:
        # Put in context with Wikipedia articles.
        topic = source
        context = GRAPH.get_or_create_concept('en', topic)
        context_normal = GRAPH.get_or_create_concept('en', *normalize_topic(topic))
        GRAPH.add_context(assertion, context_normal)
        GRAPH.get_or_create_edge('normalized', context, context_normal)
        print "in", context_normal
   
    return assertion

#def article_url_to_topic(url):
#    before, after = url.split('/wiki/', 1)
#    return urllib.unquote(after).replace('_', ' ')

def output_raw(raw_arg1, raw_arg2, raw_relation, confidence, sources=[]):
    frame = u"{1} %s {2}" % (raw_relation)
    raw = GRAPH.get_or_create_assertion(
        GRAPH.get_or_create_frame('en', frame),
        [GRAPH.get_or_create_concept('en', raw_arg1),
         GRAPH.get_or_create_concept('en', raw_arg2)],
        {'dataset': 'reverb/en', 'license': 'CC-By-SA',
         'normalized': False,
         'sources': '|'.join(sources)}
    )
    
    # Turns out that only en.wikipedia.org matters as a domain. The rest are
    # all mirrors.
    conjunction = GRAPH.get_or_create_conjunction([wikipedia, reverb])
    
    GRAPH.justify(conjunction, raw, weight=confidence)
    for source in sources:
        # Put in context with Wikipedia articles.
        #topic = article_url_to_topic(source)
        topic = source
        context = GRAPH.get_or_create_concept('en', topic)
        GRAPH.add_context(raw, context)

    #add sentence as context?
    return raw

def output_sentence(arg1, arg2, arg3, relation, raw, confidence, sources=[], prep=None):
    # arg3 is vestigial; we weren't getting sensible statements from it.
    if arg2.strip() == "": # Remove "A is for B" sentence
        return
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
    assertions = (assertion,)            
    #if not arg3 == None:
        #arg3 = normalize(arg3).strip()
        #assertion2 = GRAPH.get_or_create_assertion(
        #    GRAPH.get_or_create_concept('en', prep, 'p'),
        #    [GRAPH.get_or_create_concept('en', arg2),
        #     GRAPH.get_or_create_concept('en', arg3)],
        #    {'dataset': 'reverb/en', 'license': 'CC-By-SA',
        #     'normalized': True}
        #)
        #assertions = (assertion, assertion2)
    
    for assertion in assertions:
        conjunction = GRAPH.get_or_create_conjunction(
            [raw, reverb_object]
        )
        GRAPH.justify(conjunction, assertion, weight=confidence)
        for source in sources:
            # Put in context with Wikipedia articles.
            #topic = article_url_to_topic(source)
            topic = source
            context = GRAPH.get_or_create_concept('en', *normalize_topic(topic))
            GRAPH.add_context(assertion, context)

    return assertion

def handle_file(filename):
    import traceback
    for line in codecs.open(filename, encoding='utf-8', errors='replace'):
        line = line.strip()
        if line:
            handle_line(line)

def handle_line(line):
    parts = line.split('\t')
    if len(parts) < 10:
        return
    filename, sent_num, old_arg1, old_rel, old_arg2, \
    arg1_start, arg1_end, rel_start, rel_end, arg2_start, arg2_end, \
    confidence, sent_fragment, pos_tags, chunk_tags, \
    nor_arg1, nor_rel, nor_arg2 = parts
    # Rob put this in: skip all the numeric ones for now, our time
    # is better spent on others
    if old_arg1[0].isdigit() or old_arg2[0].isdigit():
        return
    match = ARTICLE_NAME.match(filename.split('/')[-1])
    if not match:
        return
    sources = [match.group(1)]

    
    sentence = "%s %s %s" % (old_arg1, old_rel, old_arg2)
    tokens  = sent_fragment.split('\s')
    tags = pos_tags.split('\s')
    assert(len(tokens) == len(tags))
    tokens, tags = remove_tags(tokens, tags, 'RB')	# Remove adverb
    tokens, tags = remove_tags(tokens, tags, 'MD')	# Remove modals
    tokens = map(lambda x: x.lower(), tokens)

    raw = output_raw(old_arg1, old_arg2, old_rel, confidence, sources)
    if probably_present_tense(old_rel.split()[0]):
        triple = output_triple(old_arg1, old_arg2, old_rel, raw, sources)

    index_verbs = index_of_verbs(tags)
    if len(index_verbs) == 0: return
    index_be = contain_single_be(tokens, tags)
    if index_be == len(tokens) - 1: return
    index_prep = 0
    if 'IN' in tags:
        if tags.index('IN') > index_verbs[0]:
            index_prep = tags.index('IN')
    if 'TO' in tags:
        index_to = tags.index('TO')
        if ((index_to < index_prep and index_prep > 0) or \
            (index_prep == 0)) and (index_to > index_verbs[0]):
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
                output_sentence(arg1, arg2, None, 'IsA', raw, confidence, sources)
            else:
                if tokens[index_prep] == 'of' and \
                    tokens[index_prep-1] in TYPE_WORDS:
                    # 'a kind of' frame
                    arg2 = untokenize(tokens[index_prep+1:])
                    output_sentence(arg1, arg2, None, 'IsA', raw, confidence, sources)
                elif tokens[index_prep] == 'of' and \
                    tokens[index_prep-1] == 'part':
                    # 'a part of' frame
                    arg2 = untokenize(tokens[index_prep+1:])
                    output_sentence(arg1, arg2, None, 'PartOf', raw, confidence, sources)
                else:
                    arg2 = untokenize(tokens[index_be+1:index_prep])
                    arg3 = untokenize(tokens[index_prep+1:])
                    prep = tokens[index_prep]
                    #prep_frame = 'Something can be '+untokenize([arg2, prep, arg3])
                    #print prep_frame
                    output_sentence(arg1, arg2, arg3, 'IsA', raw, confidence, sources,
                        prep=prep)
        else:
            if index_prep == 0:
                arg2 = untokenize(tokens[index_be+1:])
                output_sentence(arg1, arg2, None, 'HasProperty', raw, confidence, sources)
            else:
                arg2 = untokenize(tokens[index_be+1:index_prep])
                arg3 = untokenize(tokens[index_prep+1:])
                prep = tokens[index_prep]
                #prep_frame = 'Something can be '+untokenize([arg2, prep, arg3])
                #print prep_frame
                output_sentence(arg1, arg2, arg3, 'HasProperty', raw, confidence, sources,
                    prep=prep)
    else:
        index_be = index_of_be(tokens)
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
                output_sentence(arg1, arg2, None, relation, raw, confidence, sources)
            else:
                arg2 = untokenize(tokens[index_be+1:index_prep])
                arg3 = untokenize(tokens[index_prep+1:])
                prep = tokens[index_prep]
                #prep_frame = 'Something can be '+untokenize([arg2, prep, arg3])
                #print prep_frame
                output_sentence(arg1, arg2, arg3, relation, raw, confidence, sources,
                    prep=prep)
        else: # SubjectOf relation
            if index_prep > 0:
                arg1 = untokenize(tokens[:index_verbs[0]])
                arg2 = untokenize(tokens[index_verbs[0]:index_prep])
                arg3 = untokenize(tokens[index_prep+1:])
                prep = tokens[index_prep]
                #prep_frame = 'Something '+untokenize([arg2, prep, arg3])
                #print prep_frame
                output_sentence(arg1, arg2, arg3, 'SubjectOf', raw, confidence, sources, 
                    prep=prep)

if __name__ == '__main__':
    import sys
    for filename in sys.argv[1:]:
        if filename.startswith('reverb'):
            handle_file(filename)
