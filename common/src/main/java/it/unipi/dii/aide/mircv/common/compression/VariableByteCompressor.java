package it.unipi.dii.aide.mircv.common.compression;

import java.util.ArrayList;

import static java.lang.Math.log;

public class VariableByteCompressor {

    /**
     * Method for compressing a single integer
     * @param toBeCompressed the integer to be compressed
     * @return the compressed representation of the input number
     */
    private static byte[] integerCompression(int toBeCompressed){

        // case of the number 0
        if(toBeCompressed == 0){
            return new byte[]{0};
        }

        // compute the number of bytes needed
        int numBytes = (int) (log(toBeCompressed) / log(128)) + 1;

        // allocate the output byte array
        byte[] output = new byte[numBytes];

        // for each position (starting from the least significant) set the correct bytes
        // then divide the number by 128 to prepare the next setting
        for(int position = numBytes - 1; position >= 0; position--){
            output[position] = (byte) (toBeCompressed % 128);
            toBeCompressed /= 128;
        }

        // set the most significant bit of the least significant byte to 1
        output[numBytes - 1] += 128;
        return output;
    }

    /**
     * Method to compress an array of integers into an array of bytes using Unary compression algorithm
     * @param toBeCompressed: array of integers to be compressed
     * @return an array containing the compressed bytes
     */
    public static byte[] integerArrayCompression(int[] toBeCompressed){
        ArrayList<Byte> compressedArray = new ArrayList<>();

        // for each element to be compressed
        for(int number: toBeCompressed){
            // perform the compression and append the compressed output to the byte list
            for(byte elem: integerCompression(number))
                compressedArray.add(elem);
        }

        // transform the arraylist to an array
        byte[] output = new byte[compressedArray.size()];
        for(int i = 0; i < compressedArray.size(); i++)
            output[i] = compressedArray.get(i);

        return output;
    }

    /**
     * Method to decompress an array of bytes int an array of totNums integers using Unary compression algorithm
     * @param toBeDecompressed: array of bytes to be decompressed
     * @param totNums: total number of integers to be decompressed
     * @return an array containing the decompressed integers
     */
    public static int[] integerArrayDecompression(byte[] toBeDecompressed, int totNums){
        int[] decompressedArray = new int[totNums];

        // integer that I'm processing
        int decompressedNumber = 0;

        // count of the processed numbers (used also as a pointer in the output array)
        int alreadyDecompressed = 0;

        for(byte elem: toBeDecompressed){
            if((elem & 0xff) < 128)
                // not the termination byte, shift the actual number and insert the new byte
                decompressedNumber = 128 * decompressedNumber + elem;
            else{
                // termination byte, remove the 1 at the MSB and then append the byte to the number
                decompressedNumber = 128 * decompressedNumber + ((elem - 128) & 0xff);

                // save the number in the output array
                decompressedArray[alreadyDecompressed] = decompressedNumber;

                // increase the number of processed numbers
                alreadyDecompressed ++;

                //reset the variable for the next number to decompress
                decompressedNumber = 0;
            }
        }

        return decompressedArray;
    }
}
