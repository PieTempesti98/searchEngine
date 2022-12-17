package it.unipi.dii.aide.mircv.common.compression;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UnaryCompressorTest {

    @Test
    void integerArrayCompression() {
        assertArrayEquals(new byte[]{(byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111110}, UnaryCompressor.integerArrayCompression(new int[]{32}));
        assertArrayEquals(new byte[]{(byte) 0b11011110, (byte) 0b11111111, (byte) 0b11001111, (byte) 0b10000000}, UnaryCompressor.integerArrayCompression(new int[]{3,5,11,1,6}));
        assertArrayEquals(new byte[]{(byte) 0b01000110}, UnaryCompressor.integerArrayCompression(new int[]{1,2,1,1,3}));
    }

    @Test
    void integerArrayDecompression() {
        assertArrayEquals(new int[]{32}, UnaryCompressor.integerArrayDecompression(new byte[]{(byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111111, (byte) 0b11111110}, 1));
        assertArrayEquals(new int[]{3,5,11,1,6}, UnaryCompressor.integerArrayDecompression(new byte[]{(byte) 0b11011110, (byte) 0b11111111, (byte) 0b11001111, (byte) 0b10000000}, 5));
        assertArrayEquals(new int[]{1,2,1,1,3}, UnaryCompressor.integerArrayDecompression(new byte[]{(byte) 0b01000110}, 5));
    }
}
