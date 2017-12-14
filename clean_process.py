import csv
import os
import numpy
import re
from nltk.tokenize import TweetTokenizer
import preprocessor as p
import emoji
from pyspark import SparkContext
from StringIO import StringIO


def tweet_tokenize(t):
    t = p.clean(t)
    tknzr = TweetTokenizer()
    return tknzr.tokenize(t)

def tweet_emoji(e):
    t = emoji.demojize(e.decode('utf-8')).encode('utf-8')
    t = re.sub(r'::',': :', t)
    t = re.sub(r'[^\x00-\x7F]+',' ', text)
    return t.split()

def get_value(input1):
    k = input1[0]
    v = input1[1]
    if len(v) < 10:
        return (k,['Error'])
    else:
        return (v[9],(v[4],tweet_tokenize(v[4])))

def pri(rdd):
    for (k,v) in rdd:
        print k
        print v[0]

if __name__ == '__main__':
    sc = SparkContext()
    p.set_options(p.OPT.URL)
    # filenames = os.wholeTextFiles('sample/')
    rdd = sc.wholeTextFiles('data/2017-12-03 171635.095000/')    
    # rdd = sc.union([sc.wholeTextFiles('data/2017-12-03 171635.095000/'),sc.wholeTextFiles('data/2017-12-03 181709.587000/')])
    rdd = rdd.mapValues(lambda w:list(csv.reader(StringIO(w.encode('utf-8')), delimiter=';'))[1:])\
                .flatMapValues(lambda w:w)\
                .map(get_value)
                # .map(lambda (k,v): ((k,v[0],v[8]), tweet_tokenize(v[4])))
    pri(rdd.collect())
