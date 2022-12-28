package it.unipi.dii.aide.mircv.common.beans;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class BlockDescriptorTest {

    @Test
    void oneDescriptorBlockTest(){
        // create a posting list with 1023 elements
        PostingList list = new PostingList("test");
        for(int i = 0; i < 1023; i++){
            Posting posting = new Posting(i, ThreadLocalRandom.current().nextInt(1, 101));
            list.getPostings().add(posting);
        }
        // update block information
        VocabularyEntry voc = new VocabularyEntry("test");
        voc.updateStatistics(list);
        voc.computeBlocksInformation();

        // check the number of blocks
        assertEquals(1, voc.getNumBlocks());

        try (
            FileChannel blockChannel = (FileChannel) Files.newByteChannel(
                    Paths.get("../data/test/blockDescriptorsTest"),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
        ){
            BlockDescriptor blockDescriptor = new BlockDescriptor();
            blockDescriptor.setDocidOffset(0);
            blockDescriptor.setDocidSize(voc.getDf() * 4);
            blockDescriptor.setMaxDocid(list.getPostings().get(voc.getDf() - 1).getDocid());
            blockDescriptor.setFreqOffset(0);
            blockDescriptor.setFreqSize(voc.getDf() * 4);
            blockDescriptor.setNumPostings(list.getPostings().size());
            assertTrue(blockDescriptor.saveDescriptorOnDisk(blockChannel));

            VocabularyEntry.setTestPaths("blockDescriptorsTest");

            ArrayList<BlockDescriptor> blocks = voc.readBlocks();
            assertEquals(1, blocks.size());

            assertEquals(blockDescriptor, blocks.get(0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void multipleDescriptorsTest(){
        // create a posting list with 1025 elements
        PostingList list = new PostingList("test");
        for(int i = 0; i < 1025; i++){
            Posting posting = new Posting(i, ThreadLocalRandom.current().nextInt(1, 101));
            list.getPostings().add(posting);
        }
        // update block information
        VocabularyEntry voc = new VocabularyEntry("test");
        voc.updateStatistics(list);
        voc.computeBlocksInformation();

        // check the number of blocks
        assertEquals(33, voc.getNumBlocks());
    }

}