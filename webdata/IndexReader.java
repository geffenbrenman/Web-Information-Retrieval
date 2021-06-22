package webdata;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

public class IndexReader {
    private String dir;
    private TokensDictionary dictionary;
    private ProductIdDictionary dictionaryProduct;
    private ReviewsInformation reviewInfo;
    private int reviews;
    private int tokens;

    /**
     * Creates an IndexReader which will read from the given directory
     */
    public IndexReader(String dir) {
        this.dir = dir;
        dictionary = new TokensDictionary(dir); // tokens dictionary
        dictionaryProduct = new ProductIdDictionary(dir); // productsID dictionary
        reviewInfo = new ReviewsInformation(dir);
        reviews = dictionary.getNumOfReviews();
        tokens = dictionary.getNumOfTokens();
    }

    /**
     * Returns the product identifier for the given review
     * Returns null if there is no review with the given identifier
     */
    public String getProductId(int reviewId) {
        if ((reviewId > reviews) || (reviewId < 0)) {
            return null;
        }
        return dictionaryProduct.getProductId(reviewId - 1);
    }

    /**
     * Returns the score for a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewScore(int reviewId) {
        if ((reviewId > reviews) || (reviewId < 0)) {
            return -1;
        }
        return reviewInfo.getReviewScore(reviewId);
    }

    /**
     * Returns the numerator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessNumerator(int reviewId) {
        if ((reviewId > reviews) || (reviewId < 0)) {
            return -1;
        }
        return reviewInfo.getReviewHelpfulnessNumeratorDenominator(reviewId, true);
    }

    /**
     * Returns the denominator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessDenominator(int reviewId) {
        if ((reviewId > reviews) || (reviewId < 0)) {
            return -1;
        }
        return reviewInfo.getReviewHelpfulnessNumeratorDenominator(reviewId, false);
    }

    /**
     * Returns the number of tokens in a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewLength(int reviewId) {
        if ((reviewId > reviews) || (reviewId < 0)) {
            return -1;
        }
        return reviewInfo.getReviewLength(reviewId);
    }

    /**
     * Return the number of reviews containing a given token (i.e., word)
     * Returns 0 if there are no reviews containing this token
     */
    public int getTokenFrequency(String token) {
        return Collections.list(getReviewsWithToken(token)).size() / 2;
    }

    /**
     * Return the number of times that a given token (i.e., word) appears in
     * the reviews indexed
     * Returns 0 if there are no reviews containing this token
     */
    public int getTokenCollectionFrequency(String token) {
        int index = dictionary.searchToken(token.toLowerCase());
        if ((index <= tokens) && index >= 0) {
            return dictionary.getFrequencyOfToken(index);
        }
        return 0;
    }

    /**
     * Return the number of product reviews available in the system
     */
    public int getNumberOfReviews() {
        return reviews;
    }

    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Only return ids of reviews that include the token
     * Note that the integers should be sorted by id
     * <p>
     * Returns an empty Enumeration if there are no reviews containing this token
     */
    public Enumeration<Integer> getReviewsWithToken(String token) {
        int index = dictionary.searchToken(token.toLowerCase());
        if ((index <= tokens) && index >= 0) {
            long position = dictionary.getPostingListPosOfToken(index);
            long nextPosition = -1;
            if (index < tokens - 1) {
                nextPosition = dictionary.getPostingListPosOfToken(index + 1);
            }
            int[] reviewsFreq = dictionary.readInfoTokenByPos(Paths.get(dir, "postingLists.bin").toString(),
                    position, nextPosition);
            int prevRev = 0;
            for (int i = 0; i < reviewsFreq.length; i += 2) {
                reviewsFreq[i] += prevRev;
                prevRev = reviewsFreq[i];
            }
            Integer[] enumList = Arrays.stream(reviewsFreq).boxed().toArray(Integer[]::new);
            Vector<Integer> reviewsAndFreqs = new Vector<>(Arrays.asList(enumList));
            return reviewsAndFreqs.elements();

        }
        return new Vector<Integer>().elements();
    }

    /**
     * Return the number of number of tokens in the system
     * (Tokens should be counted as many times as they appear)
     */
    public int getTokenSizeOfReviews() {
        return dictionary.getNumOfCollection();
    }

    /**
     * Return the ids of the reviews for a given product identifier
     * Note that the integers returned should be sorted by id
     * <p>
     * Returns an empty Enumeration if there are no reviews for this product
     */
    public Enumeration<Integer> getProductReviews(String productId) {
        int numProducts = dictionaryProduct.getNumOfProducts();
        int index = dictionaryProduct.searchToken(productId);
        if ((index <= numProducts) && index >= 0) {
            int position = dictionaryProduct.getPostingListPosOfToken(index);
            int nextPosition = -1;
            if (index < numProducts - 1) {
                nextPosition = dictionaryProduct.getPostingListPosOfToken(index + 1);
            }
            int[] reviewIdByProduct = dictionaryProduct.readInfoTokenByPos(Paths.get(dir, "productPosting.bin")
                    .toString(), position, nextPosition);
            int prevRev = 0;
            for (int i = 0; i < reviewIdByProduct.length; i++) {
                reviewIdByProduct[i] += prevRev;
                prevRev = reviewIdByProduct[i];
            }
            Integer[] enumList = Arrays.stream(reviewIdByProduct).boxed().toArray(Integer[]::new);
            Vector<Integer> allReviewsByProduct = new Vector<>(Arrays.asList(enumList));
            return allReviewsByProduct.elements();

        }
        return new Vector<Integer>().elements();
    }
}