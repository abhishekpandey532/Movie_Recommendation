from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt


class MovieRecommender(MRJob):
    def configure_options(self):
        super(MovieRecommender, self).configure_options()
        self.add_file_option('--items', help='u.item')

    def load_movie_names(self):
        self.movieNames = {}
        with open('u.item') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_user_movie_ratings,
                   reducer=self.reducer_group_user_movie_ratings),
            MRStep(mapper=self.mapper_create_combinations,
                   reducer=self.reducer_calculate_similarity_score),
            MRStep(mapper=self.mapper_sort_similar_movies,
                   mapper_init=self.load_movie_names,
                   reducer=self.reducer_group_similar_movies)
        ]

    def mapper_extract_user_movie_ratings(self, _, line):
        userID, movieID, rating, timestamp = line.split()
        yield userID, (movieID, rating)

    def reducer_group_user_movie_ratings(self, userID, movie_ratings):

        ratings = []
        for movie, rating in movie_ratings:
            ratings.append((movie, rating))
        yield userID, ratings

    def mapper_create_combinations(self, userid, movie_ratings_list):

        # make combination of all movies_ratings by the user
        # input =  user1, [(m1,2),(m2,3), (m3,4)....]
        # combinations creates a list like this
        # [((m1,2)(m2,3)), ((m1,2)(m3,4)), ((m2,3)(m3,4)) ... ]
        # make a combination of movies watched by the user,
        # so we can use same combination on other users in reducer

        for mov_rat1, mov_rat2 in combinations(movie_ratings_list, 2):
            movie1 = mov_rat1[0]
            rating1 = mov_rat1[1]

            movie2 = mov_rat2[0]
            rating2 = mov_rat2[1]

            yield (movie1, movie2), (rating1, rating2)
            yield (movie2, movie1), (rating2, rating1)

    def calculate_cosine_similarity(self, ratingPairs):

        ### Cosing similarity
        # a . b / sqrt(a^2)) .sqrt((b^2))

        sum_ab = 0
        sum_aa = 0
        sum_bb = 0
        num_pairs = 0
        score = 0

        for a, b in ratingPairs:
            sum_ab += a * b
            sum_aa += a * a
            sum_bb += b * b
            num_pairs += 1

        numerator = sum_ab
        denominator = sqrt(sum_aa) * sqrt(sum_bb)

        if denominator != 0:
            score = numerator / denominator

        return (score, num_pairs)

    def reducer_calculate_similarity_score(self, moviePair, ratingPairs):

        # input is (movies (mov1, mov2) watched by same users
        # and their ratings for that movies
        # (mov1, mov2), [(1,2), (2,3), (3,4) (2, 4).....]
        # calculate cosine similarity , but plotting each ratings as a vector:
        # so if cosine_similarity is > 9.5 means that as per user rating this
        # movies are similar or recommendable

        score, num_pairs = self.calculate_cosine_similarity(ratingPairs)

        # we are intersted only in good ratings. So if score is close to 1,
        # then good ratings.
        # also ignore if rating pair count is less than 10

        if (num_pairs > 10 and score > 0.95):
            yield moviePair, (score, num_pairs)

    def mapper_sort_similar_movies(self, moviePair, score_rating_count):

        # input is moviepair , score and ratings count
        # output the movie and corresponding paired movie rated
        # also outputs the score generated and total ratings with paired
        # movie

        try:

            movie1, movie2 = moviePair
            score, total_ratings = score_rating_count

            yield self.movieNames[int(movie1)], self.movieNames[int(movie2)] + '[' + str(score) + ']' + '[' + str(total_ratings) + ']'

        except:
            pass

    def reducer_group_similar_movies(self, movie1, similar_movies_score_count):

        # just grouping the movies

        m_movies = []

        try:
            for movie in similar_movies_score_count:
                m_movies.append(movie)

            yield movie1, m_movies

        except:
            pass


if __name__ == '__main__':
    MovieRecommender.run()