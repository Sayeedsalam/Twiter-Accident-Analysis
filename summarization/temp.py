from __future__ import unicode_literals
from __future__ import division
import math
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import spacy
from scipy.spatial.distance import cosine
from nltk.util import bigrams
from tqdm import *
import pymprog as mp
import spacy_kenlm
from summarization import tf_idf

nlp = spacy.load("en")
kenlm_model = spacy_kenlm.spaCyKenLM('coca_fulltext.clean.lm.arpa')


def find_children(bigram, node):
    '''
    Given a node, this method finds all its children
    '''
    second_word = node[1]
    children = [node for node in bigram if node[0] == second_word]
    return children


def make_bigram_graph(all_bigrams, start_node):
    '''
    Given a bigram, with a defined start node and defined end nodes, this method
    returns a dictionary which serves as a graph for that bigram
    '''
    bigram = all_bigrams[:]
    bigram_graph = {}
    # start by adding the start node
    bigram_graph[start_node] = find_children(all_bigrams, start_node)
    bigram.remove(start_node)

    nodes_to_check = []
    for i in find_children(bigram, start_node):
        nodes_to_check.append(i)

    while nodes_to_check:
        node = nodes_to_check.pop()
        if node in bigram:
            bigram_graph[node] = find_children(bigram, node)
            bigram.remove(node)
            for i in find_children(bigram, node):
                nodes_to_check.append(i)
    return bigram_graph


def breadth_first_search(bigram_graph, start_node, end_node):
    '''
    This method takes as input a graph, a start node and an end node
    and returns all paths which have a length between 10 and 16
    between the two nodes.
    '''
    graph_to_manipulate = dict(bigram_graph)

    queue = []
    paths_to_return = []
    queue.append([start_node])

    while queue:
        # get the first path from the queue
        path = queue.pop(0)
        # get the last node from the path
        node = path[-1]
        # path found
        if node == end_node:
            if (len(path) < 16) and (len(path) > 10):  # limit path length
                paths_to_return.append(path)
        # enumerate all adjacent nodes, construct a new path and push it into the queue
        for adjacent in graph_to_manipulate.get(node, []):
            new_path = list(path)
            new_path.append(adjacent)
            queue.append(new_path)
        if node in graph_to_manipulate:
            del graph_to_manipulate[node]  # prevents circular references

    return paths_to_return


def make_list(bigram_path):
    '''
    This method takes a bigram path (eg. [(u'hello', u'world'), (u'world', u'!')]) and returns
    a list of unicode (eg [u'hello', u'world', u'!')
    '''
    unicode_list = []
    unicode_list.append(bigram_path[0][0])
    unicode_list.append(bigram_path[0][1])

    for bigram in bigram_path[1:]:
        unicode_list.append(bigram[1])

    return unicode_list


def informativeness(word_path, term_matrix, vocab_to_idx):
    '''
    This method returns the cosine difference between
    a tweet path and the mean of the tf-idf term matrix

    Input = word path (as a unicode list)
    Ouptut = cosine difference (scalar value)
    '''
    tfidf_mean = np.mean(term_matrix, axis=0)

    # First, I need to construct the tf-idf vector
    tfidf_path = np.zeros(len(tfidf_mean))

    for word in word_path:
        word_idx = vocab_to_idx[word]
        tfidf_path[word_idx] = np.max(term_matrix[:, word_idx])

    cosine_difference = cosine(tfidf_mean, tfidf_path)
    return cosine_difference


def linguistic_quality(word_path):
    '''
    This method takes a word path, and returns a linguistic quality score
    '''
    L = 150
    path_string = str(" ").join([token for token in word_path])
    doc = nlp(path_string)
    ll_score = math.log(10 ** kenlm_model.get_span_score(doc), 2) / L

    return (1 / (1 - ll_score))


def content_words(i, word_paths, content_vocab):
    '''Given a word path index i (for x[i]), this method will return the indices of the words in the
    content_vocab[] array
    Note: these indices are the same as for the y variable
    '''
    path = word_paths[i]
    content_indices = []

    for word in path:
        if word in content_vocab:
            content_indices.append(content_vocab.index(word))
    return content_indices


def paths_with_content_words(j, word_paths, content_vocab):
    '''Given the index j of some content word (for content_vocab[j] or y[j])
    this method will return the indices of all tweets which contain this content word
    '''
    content_word = content_vocab[j]

    indices = []

    for i in range(len(word_paths)):
        if content_word in word_paths[i]:
            indices.append(i)

    return indices


def remove_punctuation(string_list):
    # punctuation marks
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

    # traverse the given string and if any punctuation
    # marks occur replace it with null
    new_string_list = []
    for string in string_list:
        for x in string.lower():
            if x in punctuations:
                string = string.replace(x, "")
        new_string_list.append(string)
    return new_string_list


def remove_stop_words(string_list):
    data = "All work and no play makes jack dull boy. All work and no play makes jack a dull boy."
    stopWords = set(stopwords.words('english'))
    new_string_list = []
    for string in string_list:
        wordsFiltered = []
        for w in word_tokenize(string):
            if w not in stopWords:
                wordsFiltered.append(w)
        new_string_list.append(str(" ").join([token for token in wordsFiltered]))
    return new_string_list


def summerize(tweets_df):
    print(len(tweets_df))
    #print(tweets_df['tweet_texts'][1])

    tf_idf.compute_tf_idf(tweets_df)
    term_matrix = np.load('term_matrix.npy')
    vocab_to_idx = np.load('vocab_to_idx.npy', allow_pickle=True).item()
    content_vocab = list(np.load('content_vocab.npy'))
    # tfidf_dict = np.load('tfidf_dict.npy', allow_pickle=True).item()


    print("1 ##################")

    spacy_tweets = []

    for doc in nlp.pipe(tweets_df['tweet_texts'].astype('unicode'), n_threads=-1):
        spacy_tweets.append(doc)
    spacy_tweets = [tweet for tweet in spacy_tweets if len(tweet) > 1]
    # spacy_tweets = np.random.choice(spacy_tweets, 10, replace=False)
    # spacy_tweets = spacy_tweets[:20]
    print(len(spacy_tweets))
    print(spacy_tweets[0])

    print("2 ##################")

    all_bigrams = [list(bigrams([token.lemma_ for token in tweets])) for tweets in spacy_tweets]
    starting_nodes = [single_bigram[0] for single_bigram in all_bigrams]
    end_nodes = [single_bigram[-1] for single_bigram in all_bigrams]
    all_bigrams = [node for single_bigram in all_bigrams for node in single_bigram]
    all_bigrams = list(set(all_bigrams))
    print("all_bigrams len=", len(all_bigrams))
    print(all_bigrams[0])

    print("3 ##################")

    # bigram_graph = make_bigram_graph(all_bigrams, starting_nodes[1])
    # print(len(bigram_graph))
    # print(bigram_graph)
    # path = breadth_first_search(bigram_graph, starting_nodes[1], end_nodes[2])
    # print(path)

    bigram_paths = []

    for single_start_node in tqdm(starting_nodes):
        bigram_graph = make_bigram_graph(all_bigrams, single_start_node)
        for single_end_node in end_nodes:
            possible_paths = breadth_first_search(bigram_graph, single_start_node, single_end_node)
            for path in possible_paths:
                bigram_paths.append(path)
    print("bigram_paths len=", len(bigram_paths))
    # print(bigram_paths[10])

    # for tweet in spacy_tweets:
    #     bigram_paths.append(list(bigrams([token.lemma_ for token in tweets])))
    word_paths = []
    for path in tqdm(bigram_paths):
        word_paths.append(make_list(path))
    print(word_paths[0])

    print("4 ##################")


    mp.begin('COWABS')
    # Defining my first variable, x
    # This defines whether or not a word path is selected
    x = mp.var(str('x'), len(word_paths), bool)
    # Also defining the second variable, which defines
    # whether or not a content word is chosen
    y = mp.var(str('y'), len(content_vocab), bool)


    mp.maximize(
        sum([linguistic_quality(word_paths[i]) * informativeness(word_paths[i], term_matrix, vocab_to_idx) * x[i] for i in range(len(x))]) + sum(
            y))
    # hiding the output of this line since its a very long sum
    # sum([x[i] * len(word_paths[i]) for i in range(len(x))]) <= 150

    for j in range(len(y)):
        sum([x[i] for i in paths_with_content_words(j, word_paths, content_vocab)]) >= y[j]

    for i in range(len(x)):
        sum(y[j] for j in content_words(i, word_paths, content_vocab)) >= len(content_words(i, word_paths, content_vocab)) * x[i]
    mp.solve()
    result_x = [value.primal for value in x]
    result_y = [value.primal for value in y]
    mp.end()

    chosen_paths = np.nonzero(result_x)
    chosen_words = np.nonzero(result_y)
    print("*** Total = ", len(chosen_paths[0]))

    min_cosine_sim = 999
    final_sentence = None
    for i in chosen_paths[0]:
        print('--------------')
        print(str(" ").join([token for token in word_paths[i]]))
        cosine_sim = informativeness(word_paths[i], term_matrix, vocab_to_idx)
        print(cosine_sim)
        if min_cosine_sim > cosine_sim:
            min_cosine_sim = cosine_sim
            final_sentence = str(" ").join([token for token in word_paths[i]])

    # print("####### Summary ###########")
    # print(final_sentence)

    return final_sentence

if __name__ == '__main__':
    # read_tweets = pd.read_csv('stripped_filled_tweets.csv', encoding='ISO-8859-1')
    # summerize(read_tweets)

    tweets = ['BREAKING NEWS: New York radio legend Angie Martinez involved in severe car accident. Currently recovering from frac…',
              'New York radio personality Angie Martinez recovering after suffering multiple injuries in ""severe car accident""',
              'Radio Host Angie Martinez Suffers Shattered Vertebrae in Severe Car Accident\n\nAngie Martinez is in recovery after s…',
              'New York radio legend Angie Martinez involved in severe car accident. Currently recovering from fractured lumbar and shattered vertebrae']

    # tweets = ['Vernon, NY – Five Teens Injured in NY-26 Car Accident',
    #           '* A man driving drunk left the scene of an accident following a crash that injured a pedestrian in ,…',
    #           'Man dies, injured on Lagos-Ibadan Eressway accident - …',
    #           'One killed, nine others injured in Lagos-Ibadan Eressway accident - The Punch',
    #           'Man dies, nine injured on Lagos-Ibadan Eressway accident',
    #           'Overtaking: Man dies, injured in Lagos-Ibadan E/Way accident']

    new_tweets = remove_punctuation(tweets)
    new_tweets = remove_stop_words(new_tweets)
    print(new_tweets)


    df = pd.DataFrame(new_tweets, columns=['tweet_texts'])
    print(df)
    summerize(df)
