package it.unipi.dii.aide.mircv.common.beans;

import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DocumentIndexEntryTest {

    @Test
    void writeReadFromZero() {
        DocumentIndexEntry.setTestPath();
        DocumentIndexEntry entry1 = new DocumentIndexEntry("test1", 0, 10);

        long offset1 = entry1.writeToDisk();
        assertEquals(0, offset1);

        DocumentIndexEntry readEntry1 = new DocumentIndexEntry();
        assertTrue(readEntry1.readFromDisk(offset1));

        assertEquals(entry1.toString(), readEntry1.toString());
    }

    @Test
    void writeReadSubsequent() {
        DocumentIndexEntry.setTestPath();
        DocumentIndexEntry entry1 = new DocumentIndexEntry("test1", 0, 10);
        DocumentIndexEntry entry2 = new DocumentIndexEntry("test2", 1, 15);

        long offset1 = entry1.writeToDisk();
        assertEquals(0, offset1);
        long offset2 = entry2.writeToDisk();
        assertEquals(72, offset2);

        DocumentIndexEntry readEntry1 = new DocumentIndexEntry();
        assertTrue(readEntry1.readFromDisk(offset1));
        DocumentIndexEntry readEntry2 = new DocumentIndexEntry();
        assertTrue(readEntry2.readFromDisk(offset2));

        assertEquals(entry1.toString(), readEntry1.toString());
        assertEquals(entry2.toString(), readEntry2.toString());
    }

}