import json
import numpy as np
from scipy.spatial.distance import cdist
from mongo_db.mongo_db import MongoDB
import logging

from model.load import load_data
from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def loadPreadition():
    mongo_cli = MongoDB()
    predictions = mongo_cli.read_file_from_gridfs(file_name='output_part3.txt')
    get_reccomendations(predictions)


def get_reccomendations(predictions, track_id_list, top_n=5):
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')

    # Compute similarities based on predictions (simple approach: cosine similarity)
    selected_indices = df.index[df['track_id'].isin(track_id_list)].tolist()

    if len(selected_indices) == 0 or selected_indices is None:
        logger.info("No valid selected indices.")
        # return pd.DataFrame()

    # Attempt to get a proper slice of predictions
    try:
        selected_predictions = predictions[selected_indices, :]
    except IndexError:
        logger.info("IndexError with selected_indices:", selected_indices)
        # return pd.DataFrame()

    logger.info("Shape of selected predictions:", selected_predictions.shape)

    if selected_predictions.ndim == 1:  # If still 1D, reshape to maintain two dimensions
        selected_predictions = selected_predictions.reshape(1, -1)

    avg_selected_predictions = np.mean(selected_predictions, axis=0)
    logger.info("Shape of average selected predictions:", avg_selected_predictions.shape)

    # Ensure avg_selected_predictions is a 2-dimensional array
    avg_selected_predictions_2d = avg_selected_predictions.reshape(1, -1)
    logger.info("Shape of avg_selected_predictions_2d:", avg_selected_predictions_2d.shape)

    try:
        similarities = cdist(avg_selected_predictions_2d, predictions, metric='cosine')
        logger.info("Successfully calculated cosine similarities.")
    except ValueError as e:
        logger.info("Error in calculating cosine similarities:", e)
        # return pd.DataFrame()

    # Cosine similarity gives values in [0, 2] with 0 being most similar, so sort ascendingly
    recommended_indices = np.argsort(similarities[0])[:top_n + len(selected_indices)]
    recommended_indices = [idx for idx in recommended_indices if idx not in selected_indices][:top_n]

    recommendation = df.iloc[recommended_indices][['artist_name', 'track_name', 'track_id']].rename(columns={'track_id': 'trackId'})
    logger.info(recommendation)
    return json.dumps(recommendation.to_dict('records'))
