package webdata;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class TokensDictionary extends Dictionary {
    private int[] totalFrequencies; // total frequency for each token
    protected long[] postingLists; // positions to postingLists
    private int reviews;
    private int collection;


    /***
     * Contains all the tokens in the reviews.
     * We compressed the data using k-1 in k front coding method using k = 6. We wrote into one file
     * string that is an alphanumeric concatenation of all the products id’s appears in all the reviews.
     * For each k-successive product we removed a mutual prefix that we calculated. We have a frequencies
     * file that contains the total frequency of each token, and a file that contains positions to the
     * posting list of each token in the posting lists file.
     * @param dir - The directory to read the inverted index from
     */
    public TokensDictionary(String dir) {
        K = 24;
        concatenation = "";
        gamma = new Gamma();
        try {
            DataInputStream infoBlocksFile = new DataInputStream( new FileInputStream( Paths.get(dir, "infoBlocks.bin").toString()
                    ) );
            DataInputStream sizesFile = new DataInputStream( new FileInputStream(
                    Paths.get(dir, "sizes.bin").toString()  ) );
            DataInputStream positionsFile = new DataInputStream( new FileInputStream( Paths.get(dir, "positions.bin").toString()
                     ) );
            File totalFrequenciesFile = new File( Paths.get(dir, "frequencies.bin").toString()  );
            BufferedReader longStringFile = new BufferedReader( new FileReader( Paths.get(dir, "longString.txt").toString()
                     ) );
            tokensSize = infoBlocksFile.readInt();
            blocks = (int) Math.ceil( (double) tokensSize / K );
            reviews = infoBlocksFile.readInt();
            collection = infoBlocksFile.readInt();
            tokenPointer = new int[blocks];
            readInfoBlockFile( infoBlocksFile );
            sizeToken = new int[tokensSize];
            prefixSize = new int[tokensSize];
            readSizesFile( sizesFile );
            postingLists = new long[tokensSize];
            readPositionsFile( positionsFile );
            totalFrequencies = new int[tokensSize];
            readFrequenciesFile( totalFrequenciesFile );
            readLongString( longStringFile );
            infoBlocksFile.close();
            sizesFile.close();
            positionsFile.close();
            longStringFile.close();

        } catch (Exception e) {
            System.out.println( "Error - Constructor" );
        }
    }

    /***
     * Reads the total frequencies of each token into an array field.
     * @param totalFrequenciesFile - The file in which the frequencies are written.
     */
    private void readFrequenciesFile(File totalFrequenciesFile) {
        try {
            String frequencies = getAllBytes( totalFrequenciesFile );
            String[] totalFrequenciesCodes = getAllCodes( frequencies );
            int i = 0;
            for (String code : totalFrequenciesCodes) {
                int freq = gamma.decode( code );
                totalFrequencies[i] = freq;
                i++;
            }
        } catch (Exception e) {
            System.out.println( "Error - frequencies" );
        }

    }

    /***
     * Reads the positions of the posting lists of each token into an array field.
     * @param positionsFile - The file in which the positions are written.
     */
    protected void readPositionsFile(DataInputStream positionsFile) {
        try {
            int i = 0;
            while (i < tokensSize) {
                postingLists[i] = positionsFile.readLong();
                i++;
            }
        } catch (Exception e) {
            System.out.println( "Error positions!!" );
        }
    }

    /***
     * This function read all bytes of the given file
     * @param file - The file to read bytes from
     * @return - concatenation string of all bytes read from the given file
     */
    private String getAllBytes(File file) {
        try {
            byte[] bytes = Files.readAllBytes( file.toPath() );
            return new BigInteger( bytes ).toString( 2 );
        } catch (Exception e) {
            System.out.println( "Error - getAllBytes!!" );
        }
        return "";
    }

    /***
     * Finds the index of the token in the dictionary
     * @param token - The token to search for
     * @return The index of the given token in the dictionary
     */
    public int searchToken(String token) {
        return searchTokenRecursive( 0, blocks - 1, token.toLowerCase() );
    }

    /***
     * @return The number of reviews
     */
    public int getNumOfReviews() {
        return reviews;
    }

    /***
     * @return The number of number of tokens
     */
    public int getNumOfCollection() {
        return collection;
    }

    /***
     * @param index - Index of desired token
     * @return The frequency of the given token's index
     */
    public int getFrequencyOfToken(int index) {
        return totalFrequencies[index];
    }

    /***
     * @param index - Index of desired token
     * @return The position of the given token's positing list
     */
    public long getPostingListPosOfToken(int index) {
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
    public int[] readInfoTokenByPos(String dir, long start, long end) {
        try {
            RandomAccessFile file = new RandomAccessFile( dir, "r" );
            file.seek( start );
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
            file.seek( start );
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
