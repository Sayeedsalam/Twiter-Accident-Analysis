import spacy
from textacy.vsm import Vectorizer
import numpy as np
import pandas as pd
from tqdm import *

nlp = spacy.load("en")
main_words = [u'earthquake', u'killed', u'injured', u'stranded', u'wounded', u'hurt', u'helpless', u'wrecked', u'nepal']
useful_entities = [u'NORP', u'FACILITY', u'ORG', u'GPE', u'LOC', u'EVENT', u'DATE', u'TIME']


def compute_tf_idf(tweets_df):
    print(len(tweets_df))
    print(tweets_df.head())
    print(tweets_df["tweet_texts"][0])

    print("##################")

    spacy_tweets = []

    for doc in nlp.pipe(tweets_df["tweet_texts"].astype('unicode'), n_threads=-1):
        spacy_tweets.append(doc)
    spacy_tweets = [tweet for tweet in spacy_tweets if len(tweet) > 1]
    # spacy_tweets = spacy_tweets[:20]
    print(len(spacy_tweets))
    print(spacy_tweets[0])

    # content_tweets = []
    # for single_tweet in tqdm(spacy_tweets):
    #     single_tweet_content = []
    #     for token in single_tweet:
    #         if ((token.pos_ == u'NUM')
    #                 or (token.lower_ in main_words)):
    #             single_tweet_content.append(token)
    #     content_tweets.append(single_tweet_content)

    content_tweets = []
    for single_tweet in tqdm(spacy_tweets):
        single_tweet_content = []
        for token in single_tweet:
                single_tweet_content.append(token)
        content_tweets.append(single_tweet_content)

    vectorizer = Vectorizer(apply_idf=True)
    term_matrix = vectorizer.fit_transform([tok.lemma_ for tok in doc] for doc in spacy_tweets)
    np_matrix = term_matrix.todense()

    tfidf_dict = {}
    content_vocab = []
    for tweet in content_tweets:
        for token in tweet:
            if token.lemma_ not in tfidf_dict:
                content_vocab.append(token.lemma_)
                tfidf_dict[token.lemma_] = np.max(np_matrix[:, vectorizer.vocabulary_terms[token.lemma_]])

    for key in sorted(tfidf_dict)[0:10]:
        print("WORD:" + str(key) + " -- tf-idf SCORE:" + str(tfidf_dict[key]))

    np.save('term_matrix.npy', np_matrix)
    np.save('tfidf_dict.npy', tfidf_dict)
    np.save('content_vocab.npy', content_vocab)
    np.save('vocab_to_idx.npy', vectorizer.vocabulary_terms)

    print(np_matrix.shape)
    print(np_matrix)
    print(len(tfidf_dict.keys()))


    return np_matrix, content_vocab, vectorizer.vocabulary_terms


if __name__ == '__main__':
    read_tweets = pd.read_csv('stripped_filled_tweets.csv', encoding='ISO-8859-1')
    compute_tf_idf(read_tweets)


