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

CREATE TABLE IF NOT EXISTS countries (
    country_code CHAR(2) PRIMARY KEY,
    country_name TEXT
);

CREATE TABLE IF NOT EXISTS movie_countries (
    movie_id INTEGER,
    country_code CHAR(2),
    PRIMARY KEY (movie_id, country_code),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (country_code) REFERENCES countries(country_code)
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS languages (
    language_code CHAR(2) PRIMARY KEY,
    endonym_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_languages (
    movie_id INTEGER,
    language_code CHAR(2),
    PRIMARY KEY (movie_id, language_code),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (language_code) REFERENCES languages(language_code)
);

CREATE TABLE IF NOT EXISTS actors (
    actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    gender INTEGER CHECK (gender IN (1, 2, 3)),
    country_code CHAR(2),
    FOREIGN KEY (country_code) REFERENCES countries(country_code)
);

CREATE TABLE IF NOT EXISTS movie_cast (
    character_id INTEGER PRIMARY KEY,
    movie_id INTEGER,
    actor_id INTEGER,
    character TEXT NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER,
    movie_id INTEGER,
    score REAL CHECK (rating_score BETWEEN 0.5 AND 5),
    rating_date DATE,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

