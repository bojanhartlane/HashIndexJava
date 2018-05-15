import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 *  Database Systems - HASHED INDEXES IMPLEMENTATION
 */

public class hashquery implements dbimpl
{
    // Initialize
    public static void main(String args[])
    {
        hashquery load = new hashquery();
        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    // Reading command line arguments
    public void readArguments(String args[])
    {
        if (args.length == 2) {
            if (isInteger(args[1])) {
              readIndex(args[0].trim(), Integer.parseInt(args[1]));
            }
        }
        else {
            System.out.println("Error: only pass in two arguments");
        }
    }

    // Check if pagesize is a valid integer
    public boolean isInteger(String s)
    {
        boolean isValidInt = false;
        try {
            Integer.parseInt(s);
            isValidInt = true;
        }
        catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return isValidInt;
    }

   // Read "hash.<pagesize>" file one bucket at a time
   public void readIndex(String query, int pagesize)
   {
        File heapfile = new File(HEAP_FNAME + pagesize);
        File hashfile = new File(HASH_FNAME + pagesize);
        int recCount = 0;
        int totalBucketScanned = 0;
        
        //Buckets variables
        int totalBuckets = (MAXIMUM_RECORDS * SLOT_SIZE / pagesize);
        int modBucket = (MAXIMUM_RECORDS * SLOT_SIZE) % pagesize;
        if (modBucket > 0) {
            totalBuckets++;
        }
        int slotsInBucket = pagesize / SLOT_SIZE;    
        
        try
        {
            DataInputStream disHash = new DataInputStream(new FileInputStream(hashfile));
            FileInputStream fisHeap = new FileInputStream(heapfile);
            // Boolean to check if the matching record has been found or not
            boolean foundRecord = false;
            // Calculate the hash code of the query
            int queryHashCode = Math.abs(query.toLowerCase().hashCode());
            // Calculate the starting bucket position to search matching record
            int currentBucket = queryHashCode % totalBuckets;
            // long variable to start reading the file from the bucket location instead of from the beginning
            long skipPointer = (long)(currentBucket * pagesize);
            // Skip reading the hash file to the bucket location
            disHash.skip(skipPointer);
            
            // Keep reading until the matching record is found or until all records are scanned
            while (!foundRecord) {
                byte[] bLocation = new byte[SLOT_SIZE];
                byte[] bRecord = new byte[RECORD_SIZE];
                int location = disHash.readInt();      
                
                // Only check if the slot contains a record location
                if (location != -1) {
                    /*
                    * Move the reading pointer of the heap file to a particular record's location,
                    * read the particular record,
                    * and then move the reading pointer back to the beginning of the heap file
                    */
                    fisHeap.skip(location);
                    fisHeap.read(bRecord, 0, RECORD_SIZE);
                    fisHeap.skip((location + RECORD_SIZE) * -1);
                    // Check if the record is equal with the query or not
                    foundRecord = findRecord(bRecord, query);
                } else {
                    foundRecord = true;
                    totalBucketScanned = totalBuckets;
                }
                
                recCount++;
                if (!foundRecord) {
                    if (recCount == slotsInBucket) {
                        disHash.read(bLocation, 0, (pagesize % SLOT_SIZE));
                        currentBucket++;
                        totalBucketScanned++;                                  
                        if (totalBucketScanned == totalBuckets) {
                            foundRecord = true;
                        }
                        if (currentBucket == totalBuckets) {
                            currentBucket = 0;
                            disHash.skip((totalBuckets * pagesize) * -1);
                        }                        
                        recCount = 0;
                    }
                }
            }
            
            // Return warning message about no matching record in database
            if (foundRecord && (totalBucketScanned == totalBuckets)) {
                System.out.println("There's no matching record found in the database");
            }
            
        }
        catch (FileNotFoundException e) {
            System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
   }

    // Compare BN_NAME of a record with the query.
    // If they're a match, return true and print the record on the screen.
    // If they're not a match, return false.
    public boolean findRecord(byte[] rec, String input)
    {
        String record = new String(rec);
        // Get the BN_NAME field only and remove null characters from BN_NAME field
        String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                            RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE).replace("\0", "");
        
        if (BN_NAME.toLowerCase().equals(input.toLowerCase())) {
            System.out.println(record);
            return true;
        } else {
            return false;
        }
    }
}
