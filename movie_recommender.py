import sqlite3
import pandas as pd
import numpy as np

# Loading the csv files into dataframes
movies_df = pd.read_csv("https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Movies.csv")
persons_df = pd.read_csv("https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Persons.csv")
ratings_df = pd.read_csv("https://raw.githubusercontent.com/tugraz-isds/datasets/master/movies/Ratings.csv")

# MOVIES TABLE
# Creating the dataframe 
movies_table_df = movies_df[['MovieID','OriginalTitle', 'EnglishTitle', 'OriginalLanguage', 'ReleaseDate', 'Runtime', 
                             'Homepage', 'Budget', 'Revenue']].rename(
                                 columns={
                                     'MovieID': 'movie_id',
                                     'OriginalTitle': 'orig_title',
                                     'EnglishTitle': 'english_title',
                                     'OriginalLanguage': 'orig_language',
                                     'ReleaseDate': 'release_date',
                                     'Runtime': 'runtime',
                                     'Homepage': 'homepage',
                                     'Budget': 'budget',
                                     'Revenue': 'revenue'
                                 }
                             )
movies_table_df['budget_type'] = np.nan

# Connecting to database
conn = sqlite3.connect('movie_recommender.db')
cursor = conn.cursor()

# Creating the table in the database
cursor.execute('''
CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    orig_title TEXT NOT NULL,
    english_title TEXT,
    orig_language TEXT,
    release_date DATE,
    runtime INTEGER,
    homepage TEXT,
    budget REAL,
    revenue REAL,
    budget_type TEXT
);
''')
# Populating the database from the dataframe 
movies_table_df.to_sql('movies', conn, if_exists='append', index=False)

# COUNTRIES AND MOVIE_COUNTRIES TABLE
# This removes the null values and replaces it with empty strings- need this to use the split functions 
movies_df['ProductionCountries'] = movies_df['ProductionCountries'].fillna('')

# For each movie, now there is a list of countries instead of them separated by '|'
movies_df['ProductionCountries'] = movies_df['ProductionCountries'].str.split('|')

# This creates a row for each country in the list of countries for each movie, so now there is a row for each country for each movie 
exploded_df = movies_df.explode('ProductionCountries')
exploded_df = exploded_df[exploded_df['ProductionCountries'] != '']

# This splits up the country code and country and creates two new columns to store them 
exploded_df[['country_code', 'country_name']] = exploded_df['ProductionCountries'].str.split('-', expand=True)

# Creating the country and movie country dataframes 
countries_df = exploded_df[['country_code', 'country_name']].drop_duplicates()
movie_countries_df = exploded_df[['MovieID', 'country_code']].drop_duplicates()
movie_countries_df = movie_countries_df.rename(columns={'MovieID': 'movie_id', 'country_code': 'country_code'})

# Creating the tables and populating them from the dataframe 
cursor.execute('''
CREATE TABLE IF NOT EXISTS countries (
    country_code CHAR(2) PRIMARY KEY,
    country_name TEXT
);
''')

countries_df.to_sql('countries', conn, if_exists='append', index=False)

cursor.execute('''
CREATE TABLE IF NOT EXISTS movie_countries (
    movie_id INTEGER,
    country_code CHAR(2),
    PRIMARY KEY (movie_id, country_code),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (country_code) REFERENCES countries(country_code)
);
''')
movie_countries_df.to_sql('movie_countries', conn, if_exists='append', index=False)


# GENRES AND MOVIE_GENRES TABLES
movies_df['Genres'] = movies_df['Genres'].fillna('')
movies_df['Genres'] = movies_df['Genres'].str.split('|')
# Explode the DataFrame to separate each genre into its own row
exploded_genres_df = movies_df.explode('Genres')

exploded_genres_df = exploded_genres_df[exploded_genres_df['Genres'] != '']

# Create the 'genres_df' with unique genres and an auto-incremented genre_id
genres_df = pd.DataFrame(exploded_genres_df['Genres'].unique(), columns=['genre_name']).drop_duplicates()
genres_df['genre_id'] = genres_df.reset_index().index + 1

# Create the 'movie_genres_df' by mapping each movie to its corresponding genre_id
# Merge exploded_genres_df with genres_df to get genre_id for each genre
movie_genres_df = pd.merge(exploded_genres_df[['MovieID', 'Genres']], genres_df, left_on='Genres', right_on='genre_name')

# Select only 'movie_id' and 'genre_id' for the final movie_genres_df
movie_genres_df = movie_genres_df[['MovieID', 'genre_id']].drop_duplicates()

movie_genres_df = movie_genres_df.rename(columns={'MovieID': 'movie_id'})

# Create the tables in the database and populating them from the dataframe 
cursor.execute('''
CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT NOT NULL
);
''')

genres_df = genres_df[['genre_id', 'genre_name']]
genres_df.to_sql('genres', conn, if_exists='append', index=False)

cursor.execute('''
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);
''')
movie_genres_df.to_sql('movie_genres', conn, if_exists='append', index=False)

# LANGUAGES AND MOVIE_LANGUAGES TABLES
# Handle missing values and split 'SpokenLanguages'
movies_df['SpokenLanguages'] = movies_df['SpokenLanguages'].fillna('')
movies_df['SpokenLanguages'] = movies_df['SpokenLanguages'].str.split('|')

# Explode the list of languages into multiple rows
exploded_languages_df = movies_df.explode('SpokenLanguages')

# Filter out any rows where 'SpokenLanguages' is empty
exploded_languages_df = exploded_languages_df[exploded_languages_df['SpokenLanguages'] != '']

# Split 'SpokenLanguages' into 'language_code' and 'endonym_name'
exploded_languages_df[['language_code', 'endonym_name']] = exploded_languages_df['SpokenLanguages'].str.split('-', expand=True)

# Create 'languages_df' with unique values of 'language_code' and 'endonym_name'
languages_df = exploded_languages_df[['language_code', 'endonym_name']].drop_duplicates()

# Create 'movie_languages_df' mapping 'movie_id' to 'language_code'
movie_languages_df = exploded_languages_df[['MovieID', 'language_code']].drop_duplicates()

# Rename 'MovieID' column to 'movie_id' to match schema
movie_languages_df = movie_languages_df.rename(columns={'MovieID': 'movie_id'})

# Create tables in the database and populate them from the dataframe 
cursor.execute('''
CREATE TABLE IF NOT EXISTS languages (
    language_code CHAR(2) PRIMARY KEY,
    endonym_name TEXT
);
''')

languages_df.to_sql('languages', conn, if_exists='append', index=False)

cursor.execute('''
CREATE TABLE IF NOT EXISTS movie_languages (
    movie_id INTEGER,
    language_code CHAR(2),
    PRIMARY KEY (movie_id, language_code),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (language_code) REFERENCES languages(language_code)
);
''')
movie_languages_df.to_sql('movie_languages', conn, if_exists='append', index=False)

# ACTORS AND MOVIE_CAST TABLES
# Extract unique 'Name' and 'Gender' from persons_df
unique_actors = persons_df[['Name', 'Gender']].drop_duplicates()

# Add an auto-incrementing 'actor_id' column
unique_actors['actor_id'] = range(1, len(unique_actors) + 1)

# Add a 'country_code' column with null values
unique_actors['country_code'] = np.nan  

# Reorder the columns to match the format
actors_df = unique_actors[['actor_id', 'Name', 'Gender', 'country_code']]

# Rename the columns to match the schema
actors_df = actors_df.rename(columns={'Name': 'name', 'Gender': 'gender'})

# For the movie_cast dataframe
# Merge persons_df with actors_df to get 'actor_id' based on the 'Name' column
merged_df = pd.merge(persons_df, actors_df[['actor_id', 'name']], left_on='Name', right_on='name', how='left')

# Create 'movie_cast_df' by selecting the necessary columns
merged_df = merged_df.dropna(subset=['CastID', 'MovieID', 'actor_id', 'Character'])
merged_df = merged_df[(merged_df['CastID'] != '') & (merged_df['Character'] != '')]

# Create 'movie_cast_df' by selecting the necessary columns
movie_cast_df = merged_df[['CastID', 'MovieID', 'actor_id', 'Character']]

# Remove duplicates in 'CastID' to ensure unique 'character_id'
movie_cast_df = movie_cast_df.drop_duplicates(subset=['CastID'])

# Rename columns to match the required schema
movie_cast_df = movie_cast_df.rename(columns={
    'CastID': 'character_id',
    'MovieID': 'movie_id',
    'Character': 'character'
})

# Create the tables in the database and populate them from the dataframes 
cursor.execute('''
CREATE TABLE IF NOT EXISTS actors (
    actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    gender INTEGER CHECK (gender IN (1, 2, 3)),
    country_code CHAR(2),
    FOREIGN KEY (country_code) REFERENCES countries(country_code)
);
''')

actors_df.to_sql('actors', conn, if_exists='append', index=False)

cursor.execute('''
CREATE TABLE IF NOT EXISTS movie_cast (
    character_id TEXT PRIMARY KEY,
    movie_id INTEGER,
    actor_id INTEGER,
    character TEXT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);
''')
movie_cast_df.to_sql('movie_cast', conn, if_exists='append', index=False)


#RATINGS TABLE
ratings_df = ratings_df.rename(columns={
    'UserID': 'user_id',
    'MovieID': 'movie_id',
    'Rating': 'rating_score',
    'Date': 'rating_date'
})

cursor.execute('''
CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating_score REAL CHECK (rating_score BETWEEN 0.5 AND 5),
    rating_date DATE,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
''')

ratings_df.to_sql('ratings', conn, if_exists='append', index=False)


