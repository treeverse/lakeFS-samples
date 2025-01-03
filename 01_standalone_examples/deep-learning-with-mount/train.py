#!/usr/bin/env python
import sys
import tensorflow as tf

# input
input_location = sys.argv[1]

# output
model_location = './models/is_alpaca.keras'

def get_ds(subset):
    return tf.keras.utils.image_dataset_from_directory(
        input_location, validation_split=0.2, subset=subset,
        seed=1234, image_size=(244, 244), batch_size=32)

train_ds = get_ds("training")
val_ds = get_ds("validation")

model = tf.keras.Sequential([
    tf.keras.layers.Rescaling(1./255),
    tf.keras.layers.Conv2D(32, 3, activation='relu'),
    tf.keras.layers.MaxPooling2D(),
    tf.keras.layers.Conv2D(32, 3, activation='relu'),
    tf.keras.layers.MaxPooling2D(),
    tf.keras.layers.Conv2D(32, 3, activation='relu'),
    tf.keras.layers.MaxPooling2D(),
    tf.keras.layers.Flatten(),
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dense(2)])

# Fit and save
loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
model.compile(optimizer='adam', loss=loss_fn, metrics=['accuracy'])
model.fit(train_ds, validation_data=val_ds, epochs=3)
model.save(model_location)

