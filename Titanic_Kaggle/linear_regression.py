import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
import tensorflow as tf

def make_input(data_frame, label_df, num_epochs=10, shuffle=True, batch_size=32):
    def input_function():
        data_set = tf.data.Dataset.from_tensor_slices((dict(data_frame), label_df))
        if shuffle:
            data_set = data_set.shuffle(1000)
        data_set = data_set.batch(batch_size).repeat(num_epochs)
        return data_set
    return input_function


train_frame = pd.read_csv("C:/Users/Re(d)ginald/Documents/Kaggle Titanic/train.csv")
eval_frame = pd.read_csv("C:/Users/Re(d)ginald/Documents/Kaggle Titanic/eval.csv")
surv_train = train_frame.pop("survived")
surv_eval = eval_frame.pop("survived")

categ_col = ["sex", "n_siblings_spouses", "parch", "class", "deck",
                       "embark_town", "alone"]
num_col = ["age", "fare"]

feat_col = []
for feat_name in categ_col:
    vocab = train_frame[feat_name].unique()
    feat_col.append(tf.feature_column.categorical_column_with_vocabulary_list(feat_name, vocab))

for feat_name in num_col:
    feat_col.append(tf.feature_column.numeric_column(feat_name, dtype=tf.float32))

train_input = make_input(train_frame, surv_train)
eval_input = make_input(eval_frame, surv_eval, num_epochs=1, shuffle=False)
linear_pred = tf.estimator.LinearClassifier(feature_columns=feat_col)
linear_pred.train(train_input)
result = linear_pred.evaluate(eval_input)
print(result)

# Optional prediction of personal probabilities:
# result = list(linear_pred.predict(eval_input))
# print(result[index]["probabilities"][1]), where index is which passenger we're looking at
