package it.unipi.dii.aide.mircv.common.utils;

import java.io.File;
import java.io.PrintWriter;

public class FileUtils {

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
}
