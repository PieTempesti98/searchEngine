package it.unipi.dii.aide.mircv.common.compression;

public class UnaryCompressor {
    /**
     * Method to compress an array of integers into an array of bytes using Unary compression algorithm
     * @param toBeCompressed: array of integers to be compressed
     * @return an array containing the compressed bytes
     */
    public static byte[] integerArrayCompression(int[] toBeCompressed){

        int nBits = 0;

        // computing total number of bits to be written
        for(int i=0; i<toBeCompressed.length; i++){
            // each integer number will be compressed in a number of bits equal to its value
            nBits+=toBeCompressed[i];
        }

        // computing total number of bytes needed as ceil of nBits/8
        int nBytes = (nBits/8 + (((nBits % 8) != 0) ? 1 : 0));

        //System.out.println("total bits needed: "+ nBits+"\ttotal bytes needed: "+nBytes);

        // initialization of array for the compressed bytes
        byte[] compressedArray = new byte[nBytes];

        int nextByteToWrite = 0;
        int nextBitToWrite = 0;

        // compress each integer
        for(int i=0; i<toBeCompressed.length; i++){

            // check if integer is 0
            if(toBeCompressed[i]<=0){
                System.out.println("skipped element <=0 in the list of integers to be compressed");
                continue;
            }

            // write as many 1s as the value of the integer to be compressed -1
            for(int j=0; j<toBeCompressed[i]-1; j++){
                // setting to 1 the j-th bit starting from left
                compressedArray[nextByteToWrite] = (byte) (compressedArray[nextByteToWrite] | (1 << 7-nextBitToWrite));

                // update counters for next bit to write
                nextBitToWrite++;

                // check if the current byte as been filled
                if(nextBitToWrite==8){
                    // new byte must be written as next byte
                    nextByteToWrite++;
                    nextBitToWrite = 0;
                }
            }

            // skip a bit since we should encode a 0 (which is the default value) as last bit
            // of the Unary encoding of the integer to be compressed
            nextBitToWrite++;

            // check if the current byte as been filled
            if(nextBitToWrite==8){
                // new byte must be written as next byte
                nextByteToWrite++;
                nextBitToWrite = 0;
            }
        }

        return compressedArray;
    }

    /**
     * Method to decompress an array of bytes int an array of totNums integers using Unary compression algorithm
     * @param toBeDecompressed: array of bytes to be decompressed
     * @param totNums: total number of integers to be decompressed
     * @return an array containing the decompressed integers
     */
    public static int[] integerArrayDecompression(byte[] toBeDecompressed, int totNums){
        int[] decompressedArray = new int[totNums];

        int toBeReadedByte = 0;
        int toBeReadedBit = 0;
        int nextInteger = 0;
        int onesCounter = 0;

        // process each bit
        for(int i=0; i<toBeDecompressed.length*8; i++){

            // create a byte b where only the bit (i%8)-th is set
            byte b = 0b00000000;
            b |=  (1 << 7-(i%8));

            //System.out.println(Integer.toBinaryString(b & 255 | 256).substring(1));

            // check if in the byte to be read the bit (i%8)-th is set to 1 or 0
            if((toBeDecompressed[toBeReadedByte] & b)==0){
                // i-th bit is set to 0

                // writing the decompressed number in the array of the results
                decompressedArray[nextInteger] = onesCounter+1;

                // the decompression of a new integer ends with this bit
                nextInteger++;

                if(nextInteger==totNums)
                    break;

                // resetting the counter of ones for next integer
                onesCounter = 0;

            }
            else{
                // i-th bit is set to 1

                // increment the counter of ones
                onesCounter++;

            }

            toBeReadedBit++;

            if(toBeReadedBit==8){
                toBeReadedByte++;
                toBeReadedBit=0;
            }

        }

        return decompressedArray;
    }

}
