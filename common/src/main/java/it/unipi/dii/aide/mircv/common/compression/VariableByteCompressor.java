package it.unipi.dii.aide.mircv.common.compression;

public class VariableByteCompressor {

    /**
     * Method to compress an array of integers into an array of bytes using Unary compression algorithm
     * @param toBeCompressed: array of integers to be compressed
     * @return an array containing the compressed bytes
     */
    public static byte[] integerArrayCompression(int[] toBeCompressed){

        byte[] compressedArray = new byte[nBytes];

        // TODO: IMPLEMENTATION

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

        // TODO: IMPLEMENTATION
        return decompressedArray;
    }
}
