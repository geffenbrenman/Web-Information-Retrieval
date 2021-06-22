package webdata;

import java.util.*;
import java.util.stream.Collectors;

public class ReviewSearch{
    private final IndexReader reader;
    private final double SCORE_FACTOR = 0.5;
    private final double HELPFULNESS_FACTOR = 0.3;
    private final double NUM_REVIEWS_FACTOR = 0.2;
    private final double LAMBDA_FACTOR = 0.5;
    private final int K_FACTOR = 4;
    private final int SCORE_NORM_FACTOR = 5;

    /**
     * Constructor
     */
    public ReviewSearch(IndexReader iReader){
        reader = iReader;
    }

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the vector space ranking function lnn.ltc (using the
     * SMART notation)
     * The list should be sorted by the ranking
     */
    public Enumeration<Integer> vectorSpaceSearch(Enumeration<String> query, int k){
        HashMap<String, List<Integer>> freqs = new HashMap<>();
        HashMap<Integer, Double> scores = new HashMap<>();
        HashMap<String, Integer> queries = new HashMap<>();
        int queryLength = 0;
        while (query.hasMoreElements()){
            queryLength++;
            String token = query.nextElement().toLowerCase();
            int counter = queries.getOrDefault(token, 0);
            queries.put(token, counter + 1);
            if (!freqs.containsKey(token)){
                Enumeration<Integer> enumeration = reader.getReviewsWithToken(token);
                List<Integer> reviewsFreqs = Collections.list(enumeration);
                for (int i = 0; i < reviewsFreqs.size(); i += 2){
                    scores.put(reviewsFreqs.get(i), 0.0);
                }
                freqs.put(token, reviewsFreqs);
            }
        }
        double[] queryScores = calculateQueryScores(freqs, queries, queryLength);
        for (int reviewId : scores.keySet()){
            double score = calculateScore(freqs, queries, reviewId, queryLength, queryScores);
            scores.put(reviewId, score);
        }
        return getKBestReviews(k, scores);
    }

    /***
     * This function calculates the query vector score
     * @param freqs All the reviews and frequencies of the queries
     * @param queries The given query
     * @param queryLength The given query length
     * @return The query vector score
     */
    private double[] calculateQueryScores(HashMap<String, List<Integer>> freqs, HashMap<String, Integer>
            queries, int queryLength){
        double[] scoresQuery = new double[queryLength];
        int N = reader.getNumberOfReviews();
        int i = 0;
        double norm = 0.0;
        // calculating ltc
        for (String token : queries.keySet()){
            double tf = Math.log10(queries.get(token)) + 1.0;
            int frequency = freqs.get(token).size() / 2;
            double dfQuery = frequency > 0 ? (Math.log10(((double) N / frequency))) : 0;
            scoresQuery[i] = tf * dfQuery;
            norm += Math.pow(scoresQuery[i], 2);
            i++;
        }
        norm = Math.sqrt(norm);
        for (int j = 0; j < queryLength; j++){
            scoresQuery[j] = scoresQuery[j] / norm;
        }
        return scoresQuery;
    }

    /***
     * This function calculates the score by lnn.ltc per query and a given reviewId
     * @param freqs All the reviews and frequencies of the queries
     * @param queries The given query
     * @param reviewId The current reviewId to calculate it's score
     * @param queryLength The given query length
     * @return The score for the given reviewId
     */
    private double calculateScore(HashMap<String, List<Integer>> freqs, HashMap<String, Integer> queries,
                                  int reviewId, int queryLength, double[] queryScores){
        double[] scoresDocument = new double[queryLength];
        int i = 0;
        // calculating lnn
        for (String token : queries.keySet()){
            double tf = (double) getTf(freqs.get(token), reviewId);
            if (tf != 0){
                tf = Math.log10(tf) + 1.0;
            }
            int frequency = freqs.get(token).size() / 2;
            double dfDocument = frequency > 0 ? 1 : 0;
            scoresDocument[i] = tf * dfDocument;
            i++;
        }
        double score = 0.0;
        for (int k = 0; k < queryLength; k++){
            score += (queryScores[k] * scoresDocument[k]);
        }
        return score;
    }

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the language model ranking function, smoothed using a
     * mixture model with the given value of lambda
     * The list should be sorted by the ranking
     */
    public Enumeration<Integer> languageModelSearch(Enumeration<String> query, double lambda, int k){
        List<String> queries = new ArrayList<>();
        HashMap<String, List<Integer>> freqs = new HashMap<>();
        HashMap<Integer, Double> scores = new HashMap<>();
        int queryLength = 0;
        while (query.hasMoreElements()){
            queryLength++;
            String token = query.nextElement().toLowerCase();
            queries.add(token);
            if (!freqs.containsKey(token)){
                Enumeration<Integer> enumeration = reader.getReviewsWithToken(token);
                List<Integer> reviewsFreqs = Collections.list(enumeration);
                for (int i = 0; i < reviewsFreqs.size(); i += 2){
                    scores.put(reviewsFreqs.get(i), 0.0);
                }
                freqs.put(token, reviewsFreqs);
            }
        }
        for (int reviewId : scores.keySet()){
            double score = calculateScoreProbability(freqs, queries, reviewId, queryLength, lambda);
            scores.put(reviewId, score);
        }
        return getKBestReviews(k, scores);
    }

    /***
     * This function calculates the score by language model per query and a given reviewId
     * @param freqs All the reviews and frequencies of the queries
     * @param queries The given query
     * @param reviewId The current reviewId to calculate it's score
     * @param queryLength The given query length
     * @param lambda The given lambda
     * @return The score for the given reviewId
     */
    private double calculateScoreProbability(HashMap<String, List<Integer>> freqs, List<String> queries,
                                             int reviewId, int queryLength, double lambda){
        double[] scores = new double[queryLength];
        int i = 0;
        // collection
        int T = reader.getTokenSizeOfReviews();
        for (String token : queries){
            int tf = getTf(freqs.get(token), reviewId);
            // P(t|Md) = tfd / |d|
            double pMd = (double) tf / reader.getReviewLength(reviewId);
            int frequency = reader.getTokenCollectionFrequency(token);
            // P(t|Mc) = cft / T
            double pMc = (double) frequency / T;
            // P(q|d) = λ*P(t|Md)+(1-λ)*P(t|Mc)
            double qd = (lambda * pMd) + ((1 - lambda) * pMc);
            scores[i] = qd;
            i++;
        }
        double score = 1.0;
        for (int j = 0; j < queryLength; j++){
            score *= scores[j];
        }
        return score;
    }

    /**
     * Returns a list of the id-s of the k most highly ranked productIds for the
     * given query using a function of your choice
     * The list should be sorted by the ranking
     */
    public Collection<String> productSearch(Enumeration<String> query, int k){
        Enumeration<Integer> reviews = languageModelSearch(query, LAMBDA_FACTOR, k * K_FACTOR);
        HashMap<Integer, Double> scores = new HashMap<>();
        while (reviews.hasMoreElements()){
            int reviewId = reviews.nextElement();
            scores.put(reviewId, calculateReviewScore(reviewId));
        }
        HashMap<String, Double> products = new HashMap<>();
        for (int reviewId : scores.keySet()){
            String product = reader.getProductId(reviewId);
            if (products.containsKey(product)){
                products.put(product, products.get(product) + scores.get(reviewId));
            }else{
                products.put(product, scores.get(reviewId));
            }
        }
        return getKBestProducts(k, products);
    }

    /**
     * This function calculates the score of the given review by our unique formula
     *
     * @param reviewId The given reviewId to calculate it's score
     * @return The score for the given reviewId
     */
    private double calculateReviewScore(int reviewId){
        int score = reader.getReviewScore(reviewId) / SCORE_NORM_FACTOR;
        int numerator = reader.getReviewHelpfulnessNumerator(reviewId);
        int denominator = reader.getReviewHelpfulnessDenominator(reviewId);
        if (numerator > denominator){
            int temp = numerator;
            numerator = denominator;
            denominator = temp;
        }
        double helpfulness = denominator > 0 ? ((double) numerator / denominator) : 0;
        int N = reader.getNumberOfReviews();
        int reviewsNum = Collections.list(reader.getProductReviews(reader.getProductId(reviewId))).size() / N;
        return (SCORE_FACTOR * score) + (HELPFULNESS_FACTOR * helpfulness) +
                (NUM_REVIEWS_FACTOR * reviewsNum);
    }

    /**
     * This function returns the frequency of the given token in the given reviewId
     *
     * @param reviewsFreqs All the reviews and frequencies of the queries
     * @param reviewId     The current reviewId to calculate it's score
     * @return The frequency
     */
    private int getTf(List<Integer> reviewsFreqs, int reviewId){
        for (int i = 0; i < reviewsFreqs.size(); i += 2){
            if (reviewsFreqs.get(i) == reviewId){
                return reviewsFreqs.get(i + 1);
            }
        }
        return 0;
    }

    /**
     * This function sorts the given hash map by it's value (the scores) and takes the first k elements
     *
     * @param k      The number of elements to take
     * @param scores The unsorted scores
     * @return An Enumeration of the k most ranking reviews
     */
    private Enumeration<Integer> getKBestReviews(int k, HashMap<Integer, Double> scores){
        List<Integer> scoresSorted = scores.entrySet().stream()
                .sorted(Map.Entry.<Integer, Double>comparingByValue().reversed().thenComparing(Map.Entry::
                        getKey))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (k < scoresSorted.size())
            scoresSorted = scoresSorted.subList(0, k);
        Integer[] reviewsInt = new Integer[scoresSorted.size()];
        for (int i = 0; i < scoresSorted.size(); i++){
            reviewsInt[i] = scoresSorted.get(i);
        }
        Vector<Integer> kBestScores = new Vector<>(Arrays.asList(reviewsInt));
        return kBestScores.elements();
    }

    /**
     * This function sorts the given hash map by it's value (the scores) and takes the first k elements
     *
     * @param k      The number of elements to take
     * @param scores The unsorted scores
     * @return A List of the k most ranking products
     */
    private Collection<String> getKBestProducts(int k, HashMap<String, Double> scores){
        List<String> scoresSorted = scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed().thenComparing(Map.Entry::
                        getKey))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (k < scoresSorted.size())
            scoresSorted = scoresSorted.subList(0, k);
        return scoresSorted;
    }
}