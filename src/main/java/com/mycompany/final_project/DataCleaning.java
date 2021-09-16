package com.mycompany.final_project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
 

 
public class DataCleaning {
 
    public static void main(String[] argv) {
 
        String crunchifyCSVFile = "src/main/resourses/Wuzzuf_Jobs.csv";
 
        // Reads text from a character-input stream, buffering characters so as to provide for the
        // efficient reading of characters, arrays, and lines.
        BufferedReader crunchifyBufferReader = null;
        String crunchifyLine = "";
 
        // This class implements the Set interface, backed by a hash table (actually a HashMap instance).
        // It makes no guarantees as to the iteration order of the set; in particular, it does not guarantee that the order will
        // remain constant over time. This class permits the null element.
        HashSet<String> crunchifyAllLines = new HashSet<>();
 
        try {
            crunchifyBufferReader = new BufferedReader(new FileReader(crunchifyCSVFile));
            while ((crunchifyLine = crunchifyBufferReader.readLine()) != null) {
                if (crunchifyAllLines.add(crunchifyLine)) {
                    crunchifyLog("Processed line: " + crunchifyLine);
                } else if (!crunchifyIsNullOrEmpty(crunchifyLine)) {
                    crunchifyLog("--------------- Skipped line: " + crunchifyLine);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (crunchifyBufferReader != null) {
                try {
                    crunchifyBufferReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
 
    // Check if String with spaces is Empty or Null
    public static boolean crunchifyIsNullOrEmpty(String crunchifyString) {
 
        if (crunchifyString != null && !crunchifyString.trim().isEmpty())
            return false;
        return true;
    }
 
    // Simple method for system outs
    private static void crunchifyLog(String s) {
 
        System.out.println(s);
    }
 
}