"""
Author: Aarushi Agarwal
Purpose: Movie Recommendation System 
Date: October 25th, 2024
"""

import numpy as np 
import sqlite3
import pandas as pd

def softmax(x):
    """
    Compute softmax values for each set of scores in x. Used to normalize the values in x
    """
    exp_x = np.exp(x - np.max(x))  # Subtract max for numerical stability
    return exp_x / exp_x.sum(axis=0)


def calculate_user_preference_vector(user_ratings, movie_genre_matrix):
    """
    Calculate the user preference vector based on the user's ratings and the movie-genre matrix
    Returns np.array: The user preference vector where each element represents the user's preference for a genre
    """
    # Scale the raw ratings and computer the user preference vector
    user_ratings = softmax(user_ratings)
    user_preference_vector = np.dot(user_ratings, movie_genre_matrix)

    # Scale the user preference vector and return it 
    user_preference_vector = softmax(user_preference_vector)
    return user_preference_vector

def calculate_recommendation_vector(user_preference_vector, movie_genre_matrix):
    """
    Calculate a recommendation vector based on the user preference vector and the movie-genre matrix.
    Returns np.array: A recommendation vector where each element represents the similarity between the user's preferences and a movie.
    """
    recommendation_vector = np.dot(movie_genre_matrix, user_preference_vector)
    return recommendation_vector

def get_user_and_movies(user_id, conn):
    """
    Select a user and extract their rated and unrated movies from the database.
    """
    cursor = conn.cursor()
    # To randomly select a user
    # cursor.execute("SELECT DISTINCT user_id FROM ratings ORDER BY RANDOM() LIMIT 1")
    # user_id = cursor.fetchone()[0]

    # Get the movies that have been rated by the user 
    # Variable rated_movies will contain tuples (movie_id, orig_title, rating_score)
    cursor.execute("""
        SELECT m.movie_id, m.orig_title, r.rating_score 
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.user_id = ?
    """, (user_id,))
    rated_movies = cursor.fetchall()

    # Get 150 random movies that have NOT been rated by the user 
    # Variable unrated_movies will contain tuples (movie_id, orig_title)
    cursor.execute("""
        SELECT m.movie_id, m.orig_title 
        FROM movies m
        LEFT JOIN ratings r ON m.movie_id = r.movie_id AND r.user_id = ?
        WHERE r.movie_id IS NULL
        ORDER BY RANDOM()
        LIMIT 150;
    """, (user_id,))
    unrated_movies = cursor.fetchall()

    return user_id, rated_movies, unrated_movies

def get_all_genres(conn):
    """
    Fetch all unique genres from the 'genres' table.
    This method was needed so that the generated matrix contains a list of all the genres
    """
    query = "SELECT genre_name FROM genres"
    all_genres = pd.read_sql_query(query, conn)
    
    # Return a list of all genres
    return all_genres['genre_name'].tolist()

def get_movie_genre_matrix(conn, movie_ids, all_genres):
    """
    Given a list of movie IDs, return a binary matrix of genres for each movie.
    """

    query = """
        SELECT mg.movie_id, g.genre_name 
        FROM movie_genres mg
        JOIN genres g ON mg.genre_id = g.genre_id
        WHERE mg.movie_id IN ({})
    """.format(",".join(["?"] * len(movie_ids)))
    
    df = pd.read_sql_query(query, conn, params=movie_ids)
    
    # Create a binary matrix using pandas one-hot encoding
    genre_matrix = pd.get_dummies(df, columns=['genre_name'], prefix='', prefix_sep='')
    genre_matrix = genre_matrix.groupby('movie_id').max().fillna(0).reset_index()

    # Reindex the genre_matrix to ensure all movie_ids are present and in the correct order 
    # This is done because there are movies that the user has rated that do not have associated genres listed
    genre_matrix = genre_matrix.set_index('movie_id').reindex(movie_ids, fill_value=0).reset_index()

    # Ensure all missing genres are filled with zeroes, and rows are aligned with movie_ids
    genre_matrix = genre_matrix.reindex(columns=['movie_id'] + all_genres, fill_value=0)
    genre_matrix = genre_matrix.astype(int)
    
    return genre_matrix

def store_recommendations(user_id, recommendations, conn):
    """
    Store the computed recommendations in the database for future use.
    """
    cursor = conn.cursor()
    for _, row in recommendations.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO recommendations (user_id, movie_id, recommendation_score)
            VALUES (?, ?, ?)
        """, (user_id, row['movie_id'], row['recommendation_score']))
    conn.commit()

def get_stored_recommendations(user_id, conn):
    """
    Retrieve stored recommendations for a user from the database.
    """
    query = """
        SELECT m.orig_title, r.recommendation_score 
        FROM recommendations r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE r.user_id = ?
        ORDER BY r.recommendation_score DESC
        LIMIT 5
    """
    stored_recommendations = pd.read_sql_query(query, conn, params=(user_id,))
    return stored_recommendations

def recommend_movies(user_id, conn):
    # First, check if we already have precomputed recommendations for this user
    stored_recommendations = get_stored_recommendations(user_id, conn)
    
    if not stored_recommendations.empty:
        print(f"Retrieved precomputed recommendations for User {user_id}:")
        print(stored_recommendations['orig_title'].to_string(index=False))
        print('\n')
        return
    
    # If no precomputed recommendations exist, calculate them
    user_id, rated_movies, unrated_movies = get_user_and_movies(user_id, conn)

    """
    print("user_id: " + str(user_id))
    print(rated_movies)
    print("--------")
    """

    # Fetch all possible genres in the system
    all_genres = get_all_genres(conn)

    # Extracting binary matrix for rated movies 
    rated_movie_ids = [movie[0] for movie in rated_movies]
    rated_genre_matrix = get_movie_genre_matrix(conn, rated_movie_ids, all_genres)
    # print(rated_genre_matrix.values[:, 1:])
    # print(rated_genre_matrix)

    # Generating the user_ratings matrix 
    user_ratings = np.array([movie[2] for movie in rated_movies])

    # Calculating the user_preference_vector 
    user_preference_vector = calculate_user_preference_vector(user_ratings, rated_genre_matrix.values[:, 1:])

    # Extracting binary matrix for unrated movies 
    unrated_movie_ids = [movie[0] for movie in unrated_movies]
    unrated_genre_matrix = get_movie_genre_matrix(conn, unrated_movie_ids, all_genres)
    # print(unrated_genre_matrix.head())

    # Calculating the recommendation_vector
    recommendation_vector = calculate_recommendation_vector(user_preference_vector, unrated_genre_matrix.values[:, 1:])

    recommendations = pd.DataFrame({
        'movie_id': unrated_genre_matrix['movie_id'],
        'recommendation_score': recommendation_vector
    }).sort_values(by='recommendation_score', ascending=False)

    recommendations = recommendations.merge(
        pd.DataFrame(unrated_movies, columns=['movie_id', 'orig_title']),
        on='movie_id'
    )
    top_5_recommendations = recommendations.head(5)  # Get the top 5 movies with the highest recommendation scores

    # Store the computed recommendations in the database for future use
    store_recommendations(user_id, recommendations, conn)

    if not top_5_recommendations.empty:
        print(top_5_recommendations['orig_title'].to_string(index=False))
    else:
        print("No recommendations available.\n")



if __name__ == "__main__":
    conn = sqlite3.connect('movie_recommender.db')

    # Ensure the recommendations table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            user_id INTEGER,
            movie_id INTEGER,
            recommendation_score REAL,
            PRIMARY KEY (user_id, movie_id),
            FOREIGN KEY (user_id) REFERENCES ratings(user_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        );
    """)

    while True:
        try:
            # Prompt the user to enter a user_id
            user_input = input("Please enter a User ID (or type 'exit' to quit): ").strip()
            
            if user_input.lower() == 'exit':
                print("Exiting the program.")
                break

            # Validate the input (check if it is an integer)
            user_id = int(user_input)

            # Check if the user ID exists in the database
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ratings WHERE user_id = ?", (user_id,))
            user_exists = cursor.fetchone()[0]

            if user_exists == 0:
                print(f"Error: User ID {user_id} does not exist. Please try again.\n")
            else:
                # If the user exists, proceed to recommend movies
                print(f"Recommended Movies for User {user_id}: ")
                print("---------------------------------")
                recommend_movies(user_id, conn)
                print("\n")
                break  # Exit the loop after successful recommendation

        except ValueError:
            print("Error: Invalid input. Please enter a numeric User ID.\n")

    conn.close()



