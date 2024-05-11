import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import numpy as np
import logging

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def load_data(filename):
    """Load and preprocess data."""
    df = pd.read_csv(filename)
    df['genre'] = df['genre'].astype('category')
    df['artist_name'] = df['artist_name'].astype('category')

    # One-hot encode genres
    genre_ohe = OneHotEncoder()
    genres_encoded = genre_ohe.fit_transform(df[['genre']]).toarray()
    genre_labels = genre_ohe.categories_[0]
    df_genres = pd.DataFrame(genres_encoded, columns=genre_labels)

    # Map artist names to indices
    artist_name_le = df['artist_name'].cat.codes.values
    num_artists = df['artist_name'].nunique()

    # Prepare numerical features
    numerical_features = ['year', 'danceability', 'energy', 'key', 'loudness', 'speechiness',
                          'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms']

    X_numerical = df[numerical_features]
    scaler = StandardScaler()
    X_numerical = scaler.fit_transform(X_numerical)

    # Concatenate genre features with numerical features
    X = np.concatenate([X_numerical, df_genres], axis=1)

    return df, X, artist_name_le, num_artists
