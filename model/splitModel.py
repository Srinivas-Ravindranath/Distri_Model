from tensorflow.keras.models import load_model, Model
from tensorflow.keras.layers import Input, concatenate
import logging

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def split_model():
    # Load the full pre-trained model
    full_model = load_model('RnnModel/model.h5')

    # Define inputs
    artist_input = full_model.input[0]  # Input layer for artist
    numerical_input = full_model.input[1]  # Input layer for numerical data

    # Part 1: Embedding and Flatten for artist input
    artist_embedding = full_model.get_layer('embedding')(artist_input)
    flattened_artist = full_model.get_layer('flatten')(artist_embedding)
    model_part1 = Model(inputs=artist_input, outputs=flattened_artist)
    model_part1.save('RnnModel/model_part1.h5')

    # Part 2: Handling from concatenate to first dropout
    concatenated_input = Input(shape=(144,), name='concatenated_input')  # Ensure this shape matches the output of concatenate layer
    dense_output = full_model.get_layer('dense')(concatenated_input)
    batch_norm_output = full_model.get_layer('batch_normalization')(dense_output)
    dropout_output = full_model.get_layer('dropout')(batch_norm_output)
    model_part2 = Model(inputs=concatenated_input, outputs=dropout_output)
    model_part2.save('RnnModel/model_part2.h5')

    # Part 3: Handling from first dropout output to final model output
    dropout_input = Input(shape=(256,), name='dropout_input')  # This should match the output shape of the first dropout
    x = full_model.get_layer('dense_1')(dropout_input)
    x = full_model.get_layer('batch_normalization_1')(x)
    x = full_model.get_layer('dropout_1')(x)
    x = full_model.get_layer('dense_2')(x)
    x = full_model.get_layer('batch_normalization_2')(x)
    x = full_model.get_layer('dropout_2')(x)
    final_output = full_model.get_layer('dense_3')(x)
    model_part3 = Model(inputs=dropout_input, outputs=final_output)
    model_part3.save('RnnModel/model_part3.h5')
