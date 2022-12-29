package it.unipi.dii.aide.mircv.common.beans;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class BlockDescriptorTest {

    @Test
    void oneDescriptorBlockTest() {
        // create a posting list with 1023 elements
        PostingList list = new PostingList("test");
        for (int i = 0; i < 1023; i++) {
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
                        StandardOpenOption.CREATE)
        ) {
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
    void multipleDescriptorsTest() {
        // create a posting list with 1025 elements
        PostingList list = new PostingList("test");
        for (int i = 1; i <= 1025; i++) {
            Posting posting = new Posting(i, ThreadLocalRandom.current().nextInt(1, 101));
            list.getPostings().add(posting);
        }
        // update block information
        VocabularyEntry voc = new VocabularyEntry("test");
        voc.updateStatistics(list);
        voc.computeBlocksInformation();

        // check the number of blocks
        assertEquals(33, voc.getNumBlocks());

        int numBlocks = voc.getNumBlocks();
        int maxNumPostings = voc.getMaxNumberOfPostingsInBlock();

        int docsMemOffset = 0;
        int freqsMemOffset = 0;

        // create iterator over posting list
        Iterator<Posting> plIterator = list.getPostings().iterator();
        try (
                FileChannel descriptorChan = (FileChannel) Files.newByteChannel(
                        Paths.get("../data/test/blockDescriptorsTest"),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE)
        ) {
            ArrayList<BlockDescriptor> blockList = new ArrayList<>();
            // simulation of saving the posting lists
            for (int i = 0; i < numBlocks; i++) {
                // create a new block descriptor and update its information
                BlockDescriptor blockDescriptor = new BlockDescriptor();
                blockDescriptor.setDocidOffset(docsMemOffset);
                blockDescriptor.setFreqOffset(freqsMemOffset);

                // number of postings written in the block
                int postingsInBlock = 0;

                int alreadyWrittenPostings = i * maxNumPostings;

                // number of postings to be written in the current block
                int nPostingsToBeWritten = (Math.min((list.getPostings().size() - alreadyWrittenPostings), maxNumPostings));

                // set docs and freqs num bytes as (number of postings)*4
                blockDescriptor.setDocidSize(nPostingsToBeWritten * 4);
                blockDescriptor.setFreqSize(nPostingsToBeWritten * 4);
                while(true) {
                    // get next posting to be written to disk
                    Posting currPosting = plIterator.next();

                    // increment counter of number of postings written in the block
                    postingsInBlock++;

                    // check if currPosting is the last posting to be written in the current block
                    if (postingsInBlock == nPostingsToBeWritten) {
                        // update the max docid of the block
                        blockDescriptor.setMaxDocid(currPosting.getDocid());

                        // update the number of postings in the block
                        blockDescriptor.setNumPostings(postingsInBlock);

                        // write the block descriptor on disk
                        blockDescriptor.saveDescriptorOnDisk(descriptorChan);

                        blockList.add(blockDescriptor);

                        docsMemOffset += nPostingsToBeWritten * 4L;
                        freqsMemOffset += nPostingsToBeWritten * 4L;

                        break;
                    }
                }
            }
            VocabularyEntry.setTestPaths("blockDescriptorsTest");

            ArrayList<BlockDescriptor> blocks = voc.readBlocks();
            assertEquals(33, blocks.size());

            for(int i = 0; i < blocks.size(); i++){
                assertEquals(blockList.get(i), blocks.get(i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
