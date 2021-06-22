package webdata;
import java.math.BigInteger;
import java.util.BitSet;

/***
 * This is an encode class, which encode and decode gamma codes.
 */
public class Gamma {
    /***
     * This function encode a number into it's gamma code. The function adds overhead to the gamma
     * code and divides it into bytes.
     * @param decimalNumber - The number to encode to gamma code.
     * @return The gamma code of the given number.
     */
    public byte[] encode(int decimalNumber) {
        StringBuilder builder = new StringBuilder();
        String offset = Integer.toBinaryString( decimalNumber );
        int length = offset.length();
        offset = offset.substring( 1, length );
        builder.append("1".repeat(length - 1));
        builder.append("0");
        builder.append(offset);
        // adds overhead
        int lenOfNum = builder.length();
        int addZeros = lenOfNum % 8;
        builder = builder.reverse();
        // how much overhead did we add
        StringBuilder firstByteBuilder = new StringBuilder();
        if (addZeros != 0) {
            for (int i = 0; i < (8 - addZeros); i++) {
                firstByteBuilder.append("1");
                builder.append("0");
            }
            builder = builder.reverse();
            firstByteBuilder.append("0".repeat(Math.max(0, addZeros)));
        } else {
            firstByteBuilder.append("00000000");
        }
        firstByteBuilder.append(builder.toString());
        return new BigInteger( firstByteBuilder.toString(), 2 ).toByteArray();
    }
//    public byte[] encode(int decimalNumber) {
//        String offset = Integer.toBinaryString(decimalNumber);
//        int length = offset.length();
//        offset = offset.substring(1, length);
//        String zero = "0";
//        String one = "1";
//        String len = one.repeat(length - 1) + "0" + offset;
////        String len = "";
////        for (int i = 0; i < length - 1; i++) {
////            len = len.concat( "1" );
////        }
////        len = len.concat( "0" );
////        len = len.concat( offset );
//        // adds overhead
//        int lenOfNum = len.length();
//        int addZeros = lenOfNum % 8;
//        // how much overhead did we add
//        String firstByte;
//        if (addZeros != 0) {
//            firstByte = one.repeat(8 - addZeros) + zero.repeat(addZeros);
//            len = zero.repeat(8 - addZeros) + len;
////                len = "0".concat( len );
////            for (int i = 0; i < (8 - addZeros); i++) {
////                firstByte = firstByte.concat( "1" );
////                len = "0".concat( len );
////            }
////            for (int i = 0; i < addZeros; i++) {
////                firstByte = firstByte.concat( "0" );
////            }
//        } else {
//            firstByte = "00000000";
//        }
////        firstByte = firstByte.concat( len );
//        firstByte = firstByte + len;
////        return new BigInteger( firstByte, 2 ).toByteArray();
//        BitSet bitSet = new BitSet(firstByte.length());
//        for (int i = 0; i < firstByte.length(); ++i) {
//            if (firstByte.charAt(firstByte.length() - i - 1) == '1') {
//                bitSet.set(i);
//            }
//        }
//        return bitSet.toByteArray();
//    }

    /***
     * This function decodes a given gamma code (given without overhead).
     * @param binaryNumber - The gamma code to decode.
     * @return The number decoded from the given gamma code.
     */
    public int decode(String binaryNumber) {
        if (binaryNumber.equals( "0" )) {
            return 1;
        }
        String number = "1";
        int i = 0;
        while (binaryNumber.charAt( i ) == '1') {
            i++;
        }
        if (i > 0) {
            number = number.concat( binaryNumber.substring( i + 1 ) );
        }
        return Integer.parseInt( number, 2 );
    }
}