import json
from pprint import pprint
import preprocessor as p
from kafka import KafkaConsumer, KafkaProducer
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
import random

from keras.models import Sequential
from keras import layers

from sklearn.metrics import precision_recall_fscore_support

from keras import backend as K

def recall_m(y_true, y_pred):
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
        recall = true_positives / (possible_positives + K.epsilon())
        return recall

def precision_m(y_true, y_pred):
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
        precision = true_positives / (predicted_positives + K.epsilon())
        return precision

def f1_m(y_true, y_pred):
    precision = precision_m(y_true, y_pred)
    recall = recall_m(y_true, y_pred)
    return 2*((precision*recall)/(precision+recall+K.epsilon()))

def tweet_preprocess(tweet):
    return p.clean(tweet)

data_dir = "2CVTweets"
data_files = ["Boston2C.csv", "Brisbane2C.csv", "Dublin2C.csv", "Seattle.csv"]

data_set = []

for data_file in data_files:
    fp = open(data_dir+"/"+data_file, "r", encoding="ISO-8859-1")

    for line in fp:
        data_set.append(line.split(";")[:-1]+[data_file])



pprint(data_set)

random.shuffle(data_set)

corpus = [tweet_preprocess(x[1]) for x in data_set]
labels = [0 if x[2]=="NO" else 1 for x in data_set]
print(len(corpus))
print(sum(labels))


corpus_tr, corpus_test, labels_tr, labels_test = train_test_split(corpus, labels, shuffle=True, test_size=0.25)

vectorizer = TfidfVectorizer(max_features=1500, ngram_range=(1,2))



X_train = vectorizer.fit_transform(corpus_tr)
X_train = X_train.todense()
print(X_train.shape)
input_dim = X_train.shape[1]
X_test = vectorizer.transform(corpus_test)

model = Sequential()
model.add(layers.Dense(10, input_dim=input_dim, activation='relu'))
model.add(layers.Dense(1, activation='relu'))


model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy', f1_m,precision_m, recall_m])
model.summary()

history = model.fit(X_train, labels_tr, epochs=5,
                     verbose=False,
                      validation_data=(X_test, labels_test),
                  batch_size=10)
model.save("tweet_classifier")
loss, accuracy, f1_score, precision, recall = model.evaluate(X_test, labels_test, verbose=False)

print("Accuracy on test data", str(accuracy))
print("Test Data: ", accuracy, f1_score, precision, recall)

lagos_tweet, lagos_label = [], []

with open("lagos_data.tsv", "r") as fp:
    for line in fp:
        tweet, label = line.split("\t")
        #print(tweet, label)
        lagos_tweet.append(tweet_preprocess(tweet))
        lagos_label.append(int(label))


lagos_test = vectorizer.transform(lagos_tweet)
lagos_test = lagos_test.todense()

predicted_labels = model.predict(lagos_test)
print(predicted_labels)
print(sum(predicted_labels))
output_fp = open("tweet.txt" ,"w+")
outout_not_tweet = open("not_tweet.txt", "w+")
loss, accuracy, f1_score, precision, recall = model.evaluate(lagos_test, lagos_label, verbose=False)

true_pos = true_neg = false_pos = false_neg = 0
print("Lagos Data: ", accuracy, f1_score, precision, recall)
for i in range(len(predicted_labels)):
    if predicted_labels[i] > 1:
        output_fp.write(lagos_tweet[i]+"\n")
        if lagos_label[i] == 1:
            true_pos += 1
        else:
            false_pos += 1
    else:
        outout_not_tweet.write(lagos_tweet[i]+"\n")
        if lagos_label[i] == 0:
            true_neg += 1
        else:
            false_neg += 1

output_fp.close()
outout_not_tweet.close()

print(true_pos/(true_pos+false_neg))
print(true_pos/(true_pos+false_pos))

kafka_consumer = KafkaConsumer("tweets", bootstrap_servers='localhost:9092')
kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

accident_tweet_count = 0

output_file = open("accident_tweets_id.txt", "w+")

for message in kafka_consumer:
    print("Received message")
    print(str(message.value))
    tweet = json.loads(message.value.decode("utf-8"))

    cleaned_text = p.clean(tweet["text"])

    vector = vectorizer.transform([cleaned_text])

    tweet["text_cleaned"] = cleaned_text

    predictions = model.predict([vector])



    if predictions[0] > 1:
        #print("Accident Related Tweet Found")
        #kafka_producer.send("accident_tweet", value=bytes(json.dumps(tweet), encoding="utf-8"))
        accident_tweet_count += 1
        output_file.write(tweet["id"]+"\n")
        output_file.flush()
        print("Number of accident related tweets found, ", accident_tweet_count)
    else:
        print("Not an accident related tweet")
        print(cleaned_text)










