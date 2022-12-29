package it.unipi.dii.aide.mircv.common.compression;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VariableByteCompressorTest {

    @Test
    void integerArrayCompression() {
        assertArrayEquals(new byte[]{(byte) 133}, VariableByteCompressor.integerArrayCompression(new int[]{5}));
        assertArrayEquals(new byte[]{(byte) 6, (byte) 184}, VariableByteCompressor.integerArrayCompression(new int[]{824}));
        assertArrayEquals(new byte[]{(byte) 6, (byte) 184,(byte) 133}, VariableByteCompressor.integerArrayCompression(new int[]{824, 5}));
    }

    @Test
    void integerArrayDecompression() {
        assertArrayEquals(new int[]{5}, VariableByteCompressor.integerArrayDecompression(new byte[]{(byte) 133}, 1));
        assertArrayEquals(new int[]{824}, VariableByteCompressor.integerArrayDecompression(new byte[]{(byte) 6, (byte) 184}, 1));
        assertArrayEquals(new int[]{824, 5}, VariableByteCompressor.integerArrayDecompression(new byte[]{(byte) 6, (byte) 184,(byte) 133}, 2));
    }

}