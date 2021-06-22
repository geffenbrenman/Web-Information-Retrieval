package webdata;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;

/***
 * Returns general information about specific review from the written files.
 */
public class ReviewsInformation {
    private String dir;

    /**
     * Returns the score for a given review
     * Returns -1 if there is no review with the given identifier
     */
    public ReviewsInformation(String directory) {
        dir = directory;
    }

    public int getReviewScore(int reviewId) {
        int score = 0;
        try {
            RandomAccessFile scores = new RandomAccessFile( Paths.get(dir, "scores.bin").toString(), "r" );
            scores.skipBytes( reviewId - 1 );
            score = scores.read();
            scores.seek( 0 );
            scores.close();
        } catch (Exception e) {
            System.out.println( "Error in score" );
        }
        return score;
    }

    /**
     * Returns the numerator or denominator (according to isNumerator flag) for the helpfulness of a given
     * review Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessNumeratorDenominator(int reviewId, boolean isNumerator) {
        byte[] numerator = new byte[2];
        try {
            RandomAccessFile helpfulness = new RandomAccessFile(
                    Paths.get(dir, "helpfulness.bin").toString(), "r" );
            if (isNumerator) {
                helpfulness.skipBytes( (reviewId - 1) * 4 );
            } else {
                helpfulness.skipBytes( ((reviewId - 1) * 4) + 2 );
            }
            numerator[0] = helpfulness.readByte();
            numerator[1] = helpfulness.readByte();
            helpfulness.seek( 0 );
            helpfulness.close();
        } catch (Exception e) {
            System.out.println( "Error in helpfulness" );
        }
        return new BigInteger( numerator ).intValue();
    }

    /**
     * Returns the number of tokens in a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewLength(int reviewId) {
        int length = 0;
        try {
            RandomAccessFile reviewLengths = new RandomAccessFile( Paths.get(dir, "reviewLengths.bin").toString(),
                    "r" );
            reviewLengths.skipBytes( (reviewId - 1) * 4 );
            length = reviewLengths.readInt();
            reviewLengths.seek( 0 );
            reviewLengths.close();
        } catch (Exception e) {
            System.out.println( "Error in reviewLengths" );
        }
        return length;
    }
}
