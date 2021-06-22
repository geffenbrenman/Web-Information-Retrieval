package webdata;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ProductIdDictionary extends Dictionary {
    protected int[] postingLists; // positions to postingLists
    protected int[] locationsReviews;
    protected String dir_index;

    /***
     * Contains all the products of the reviews.
     * We compressed the data using k-1 in k front coding method using k = 6. We wrote into one file
     * string that is an alphanumeric concatenation of all the products id’s appears in all the reviews.
     * For each k-successive product we removed a mutual prefix that we calculated. We have a frequencies
     * file that contains the total frequency of each productId, and a file that contains positions to the
     * posting list of each productId in the posting lists file.
     * @param dir - The directory to read the inverted index from
     */
    public ProductIdDictionary(String dir) {
        K = 6;
        dir_index = dir;
        concatenation = "";
        gamma = new Gamma();
        try {
            DataInputStream infoBlocksFile = new DataInputStream( new FileInputStream( Paths.get(dir, "infoBlocksProduct.bin")
                    .toString()) );
            DataInputStream sizesFile = new DataInputStream(new FileInputStream( Paths.get(dir, "sizesProduct.bin")
                    .toString() ) );
            DataInputStream positionsFile = new DataInputStream( new FileInputStream( Paths.get(dir, "positionsProduct.bin")
                    .toString()) );
            DataInputStream locationsFile = new DataInputStream( new FileInputStream( Paths.get(dir, "locationsLongString.bin")
                    .toString()) );
            BufferedReader longStringFile = new BufferedReader( new FileReader( Paths.get(dir, "longStringProduct.txt")
                    .toString()) );
            tokensSize = infoBlocksFile.readInt();
            blocks = (int) Math.ceil( (double) tokensSize / K );
            tokenPointer = new int[blocks];
            readInfoBlockFile( infoBlocksFile );
            sizeToken = new int[tokensSize]; // size of each product
            prefixSize = new int[tokensSize]; // size of each product prefix
            readSizesFile( sizesFile );
            postingLists = new int[tokensSize];
            locationsReviews = new int[tokensSize];
            readPositionsFile( positionsFile );
            readLongString( longStringFile );
            readLocationsFile( locationsFile );
            infoBlocksFile.close();
            sizesFile.close();
            positionsFile.close();
            locationsFile.close();
            longStringFile.close();
        } catch (Exception e) {
            System.out.println( "Error - Constructor Product" );
        }
    }


    /***
     * Reads the pointers in block of each product into an array field.
     * @param locationsFile - The file in which the locations are written.
     */
    protected void readLocationsFile(DataInputStream locationsFile) {
        try {
            int i = 0;
            while (i < tokensSize) {
                locationsReviews[i] = locationsFile.readInt();
                i++;
            }
        } catch (Exception e) {
            System.out.println( "Error - locations of products" );
        }
    }

    /***
     * Finds the index of the productId in the dictionary
     * @param token - The productId to search for
     * @return The index of the given productId in the dictionary
     */
    public int searchToken(String token) {
        return searchTokenRecursive( 0, blocks - 1, token );
    }

    /***
     * @return The number of productsIds
     */
    public int getNumOfProducts() {
        return tokensSize;
    }


    /***
     * Finds the product id connected to the given reviewId
     * @param reviewID - The reviewId which to find it's product id
     * @return The product id connected to the given reviewId
     */
    public String getProductId(int reviewID) {
        String productOfReview = "";
        int product = getProductIndex( reviewID + 1 );
        int locationProduct = locationsReviews[product];
        int currProductPrefix = prefixSize[product];
        productOfReview = productOfReview.concat( concatenation.substring( locationProduct,
                locationProduct + (10 - currProductPrefix) ) );
        while ((productOfReview.length() < 10) && (currProductPrefix != 0)) {
            product--;
            if (currProductPrefix > prefixSize[product]) {
                int diff = currProductPrefix - prefixSize[product];
                locationProduct = locationsReviews[product];
                productOfReview = concatenation.substring( locationProduct,
                        locationProduct + diff ).concat( productOfReview );
                currProductPrefix = prefixSize[product];
            }
        }
        return productOfReview;
    }

    /***
     * Finds the index of the given review id's product
     * @param reviewID - The reviewId which to find it's product id
     * @return The index in the dictionary of the product that connected to the given reviewId
     */
    protected int getProductIndex(int reviewID) {
        for (int i = 0; i < tokensSize - 1; i++) {
            int[] allReviews = readInfoTokenByPos(Paths.get(dir_index, "productPosting.bin").toString(),
                    postingLists[i], postingLists[i + 1] );
            int prevRev = 0;
            for (int k = 0; k < allReviews.length; k++) {
                allReviews[k] += prevRev;
                prevRev = allReviews[k];
            }
            for (int j = 0; j < allReviews.length; j++) {
                if (allReviews[j] == reviewID) {
                    return i;
                }
            }
        }
        int[] allReviews = readInfoTokenByPos( Paths.get(dir_index, "productPosting.bin").toString(),
                postingLists[tokensSize - 1], -1 );
        int prevRev = 0;
        for (int k = 0; k < allReviews.length; k++) {
            allReviews[k] += prevRev;
            prevRev = allReviews[k];
        }
        for (int j = 0; j < allReviews.length; j++) {
            if (allReviews[j] == reviewID) {
                return tokensSize - 1;
            }
        }
        return -1;
    }

    /***
     * Reads the positions of the posting lists of each token into an array field.
     * @param positionsFile - The file in which the positions are written.
     */
    protected void readPositionsFile(DataInputStream positionsFile) {
        try {
            int i = 0;
            while (i < tokensSize) {
                postingLists[i] = positionsFile.readInt();
                i++;
            }
        } catch (Exception e) {
            System.out.println( "Error positions!!" );
        }
    }

    /***
     * @param index - Index of desired token
     * @return The position of the given token's positing list
     */
    public int getPostingListPosOfToken(int index) {
        return postingLists[index];
    }

    /***
     * Read bytes from a given file by the given positions
     * @param dir - The directory in which the desired file is in
     * @param start - Start position to read from
     * @param end - End position to read until
     * @return An int array that contains all numbers written in the given file from position start to
     * position end
     */
    public int[] readInfoTokenByPos(String dir, int start, int end) {
        try {
            RandomAccessFile file = new RandomAccessFile( dir, "r" );
            file.skipBytes( start );
            // if there is no end position - end of file
            if (end == -1) {
                ArrayList<Byte> bytes = new ArrayList<>();
                while (true) {
                    try {
                        byte curByte = file.readByte();
                        bytes.add( curByte );
                    } catch (EOFException e) {
                        break;
                    }
                }
                file.seek( 0 );
                file.close();
                byte[] bytesArray = new byte[bytes.size()];
                for (int i = 0; i < bytes.size(); i++) {
                    bytesArray[i] = bytes.get( i );
                }
                String numbers = new BigInteger( bytesArray ).toString( 2 );
                String[] codes = getAllCodes( numbers );
                return decodeGammaCodes( codes );
            }
            file.seek( 0 );
            file.close();
        } catch (Exception e) {
            System.out.println( "Error - reading file by position" );
        }
        byte[] bytesArray = new byte[(int)(end - start)];
        try {
            RandomAccessFile file = new RandomAccessFile( dir, "r" );
            file.skipBytes( start );
            for (int i = 0; i < (end - start); i++) {
                bytesArray[i] = file.readByte();
            }
            file.seek( 0 );
            file.close();
        } catch (Exception e) {
            System.out.println( "Error - reading file by position" );
        }
        String numbers = new BigInteger( bytesArray ).toString( 2 );
        String[] codes = getAllCodes( numbers );
        return decodeGammaCodes( codes );
    }

}
