from __future__ import absolute_import, print_function, unicode_literals

import pandas as pd
import tensorflow as tf
import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"


def input_fn(features, labels, training=True, batch_size=256):
    # Convert the inputs to a Dataset.
    dataset = tf.data.Dataset.from_tensor_slices((dict(features), labels))

    # Shuffle and repeat if you are in training mode.
    if training:
        dataset = dataset.shuffle(1000).repeat()

    return dataset.batch(batch_size)


def pred_fn(features, batch_size=256):
    # Convert the inputs to a Dataset without labels.
    return tf.data.Dataset.from_tensor_slices(dict(features)).batch(batch_size)


col_names = ["SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species"]
species = ["Setosa", "Versicolor", "Virginica"]

traindf = pd.read_csv("C:/Users/Re(d)ginald/Documents/TensorFlow Project/Iris Flower/iris_training.csv",
                      names=col_names,
                      header=0)
testdf = pd.read_csv("C:/Users/Re(d)ginald/Documents/TensorFlow Project/Iris Flower/iris_test.csv",
                     names=col_names,
                     header=0)

train_spec = traindf.pop("Species")
test_spec = testdf.pop("Species")

feat_col = []
for key in traindf.keys():
    feat_col.append(tf.feature_column.numeric_column(key=key))

classifier = tf.estimator.DNNClassifier(feature_columns=feat_col,
                                        hidden_units=[30, 10],
                                        n_classes=3)

classifier.train(
    input_fn=lambda: input_fn(traindf, train_spec, training=True),
    steps=5000
)

result = classifier.evaluate(
    input_fn=lambda: input_fn(testdf, test_spec, training=False))
print("\nTest Set Accuracy: {accuracy:0.3f}\n".format(**result))

print("Please type numeric values as prompted.")

features = ['SepalLength', 'SepalWidth', 'PetalLength', 'PetalWidth']
predict = {}

for feature in features:
    valid = True
    while valid:
        val = input(feature + ": ")
        if not val.isdigit(): valid = False

    predict[feature] = [float(val)]

predictions = classifier.predict(input_fn=lambda: pred_fn(predict))
for pred_dict in predictions:
    class_id = pred_dict['class_ids'][0]
    probability = pred_dict['probabilities'][class_id]

    print('Prediction is "{}" ({:.1f}%)'.format(
        species[class_id], 100 * probability))
