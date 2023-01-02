package it.unipi.dii.aide.mircv.common.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * utility class used to create, clear and remove files
 */
public class FileUtils {

    /**
     * creates the file if not exists
     * @param path is the path of the file to be created
     */
    public static void createIfNotExists(String path) {
        File file = new File(path);
        try {
            file.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * removes a file
     *
     * @param path the path of the file to be removed
     */
    public static void removeFile(String path) {
        File file = new File(path);
        if (file.exists())
            file.delete();
    }

    /**
     * @param path directory path
     *             Creates a directory of given path
     **/
    public static void createDirectory(String path) {
        try {

            Path dirPath = Paths.get(path);

            Files.createDirectories(dirPath);

        } catch (IOException e) {

            e.printStackTrace();

        }
    }

    /**
     * @param path directory path
     *  Deletes directory of given path
     * **/
    public static void deleteDirectory(String path) {
        File directory = new File(path);

        if (!directory.exists())
            return;
        boolean successful = true;
        //Before deleting the directory, delete all files inside
        for (File file : Objects.requireNonNull(directory.listFiles()))
            successful = file.delete();

        if (!successful)
            return;
        //delete directory
        successful = directory.delete();


    }

}
