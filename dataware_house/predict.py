import os
print("当前工作目录：", os.getcwd())

import tensorflow as tf
import os

print("当前工作目录：", os.getcwd())

# Loading model
model_path = 'serving_dir/tip_model/1'

# 直接用 saved_model.load
loaded = tf.saved_model.load(model_path)
predict_fn = loaded.signatures["serving_default"]

# 2. Preparing data
input_data = {
    "passenger_count": tf.constant([1], dtype=tf.float32),
    "trip_distance": tf.constant([12.2], dtype=tf.float32),
    "PULocationID": tf.constant(["193"]),
    "DOLocationID": tf.constant(["264"]),
    "payment_type": tf.constant(["2"]),
    "fare_amount": tf.constant([20.4], dtype=tf.float32),
    "tolls_amount": tf.constant([0.0], dtype=tf.float32)
}

# 3. Predicting
result = predict_fn(**input_data)
print("Prediction result:", result)