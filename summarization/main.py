from sklearn.feature_extraction.text import TfidfVectorizer

corpus = [
    'This is the first document.',
    'This document is the second document.',
    'And this is the third one.',
    'Is this the first document?',
]
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(corpus)
print(vectorizer.get_feature_names())

print(X.shape)

print(X)


import pymprog as mp
mp.begin('bike production')
x, y = mp.var('x, y') # variablesas mp
mp.maximize(15 * x + 10 * y, 'profit')
x <= 3 # mountain bike limit
y <= 4 # racer production limit
x + y <= 5 # metal finishing limit
mp.solve()

print("#####################")

import spacy
import spacy_kenlm

nlp = spacy.load('en_core_web_sm')

kenlm_model = spacy_kenlm.spaCyKenLM('coca_fulltext.clean.lm.arpa')  # default model from test.arpa

nlp.add_pipe(kenlm_model)

str = "I am a boy ."
doc = nlp(str)

# doc score
print(doc._.kenlm_score)

# span score
print(doc[:2]._.kenlm_score)

# token score
print(doc[2]._.kenlm_score)

print(kenlm_model.get_full_scores(doc))
print(kenlm_model.get_span_score(doc))
