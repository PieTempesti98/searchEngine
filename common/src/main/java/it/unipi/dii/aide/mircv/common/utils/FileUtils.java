package it.unipi.dii.aide.mircv.common.utils;

import java.io.File;
import java.io.PrintWriter;

public class FileUtils {

    /**
     * creates the file if not exists, else it flushes it
     * @param path is the path of the file to be created or flushed
     */
    public static void createOrCleanFile(String path){
        File file = new File(path);
        try{
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
                try(PrintWriter writer = new PrintWriter(path)){
                    writer.print("");
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * creates the file if not exists
     * @param path is the path of the file to be created
     */
    public static void createIfNotExists(String path){
        File file = new File(path);
        try{
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File " + file.getName() + " already exists.");
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void removeFile(String path){
        File file = new File(path);
        if(file.exists())
            file.delete();
    }

}
