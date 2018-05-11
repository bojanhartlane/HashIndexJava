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
    // initialize
    public static void main(String args[])
    {
        hashquery load = new hashquery();
        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    // reading command line arguments
    public void readArguments(String args[])
    {
        if (args.length == 2) {
            if (isInteger(args[1])) {
              readIndex(args[0], Integer.parseInt(args[1]));
            }
        }
        else {
            System.out.println("Error: only pass in two arguments");
        }
    }

    // check if pagesize is a valid integer
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

   // read heapfile by page
   public void readIndex(String query, int pagesize)
   {
        File heapfile = new File(HEAP_FNAME + pagesize);
        File hashfile = new File(HASH_FNAME + pagesize);
        int recCount = 0;
        int totalBucketScanned = 0;
        
        //Buckets variables
        int totalBuckets = (MAXIMUM_RECORDS * INDEX_SIZE / pagesize);
        int modBucket = (MAXIMUM_RECORDS * INDEX_SIZE) % pagesize;
        if (modBucket > 0) {
            totalBuckets++;
        }
        int recordsInPage = pagesize / INDEX_SIZE;    
        
        try
        {
            FileInputStream fisHash = new FileInputStream(hashfile);
            FileInputStream fisHeap = new FileInputStream(heapfile);
            boolean foundRecord = false;
            
            int queryHashCode = Math.abs(query.toLowerCase().hashCode());
            int currentBucket = queryHashCode % totalBuckets;
            long skipPointer = (long)(currentBucket * pagesize);
            fisHash.skip(skipPointer);
            
            while (!foundRecord) {
                byte[] bLocation = new byte[INDEX_SIZE];
                byte[] bRecord = new byte[RECORD_SIZE];                
                fisHash.read(bLocation, 0, INDEX_SIZE);
                String locationString = new String(bLocation).replace("\0", "");
                
                if (isInteger(locationString)) {
                    long location = Long.parseLong(locationString);                                    
                    if (location != -1) {
                        fisHeap.skip(location);
                        fisHeap.read(bRecord, 0, RECORD_SIZE);
                        fisHeap.skip((location + RECORD_SIZE) * -1);
                        foundRecord = findRecord(bRecord, query);
                    }
                }
                recCount++;
                if (!foundRecord) {
                    if (recCount == recordsInPage) {
                        fisHash.read(bLocation, 0, (pagesize % INDEX_SIZE));
                        currentBucket++;
                        totalBucketScanned++;                                  
                        if (totalBucketScanned == totalBuckets) {
                            foundRecord = true;
                        }
                        if (currentBucket == totalBuckets) {
                            currentBucket = 0;
                            fisHash.skip((totalBuckets * pagesize) * -1);
                        }                        
                        recCount = 0;
                    }
                }
            }
            
            if (foundRecord && (totalBucketScanned == totalBuckets)) {
                System.out.println("There's no matching record in the database");
            }
            
        }
        catch (FileNotFoundException e) {
            System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
   }

    // returns records containing the argument text from shell
    public boolean findRecord(byte[] rec, String input)
    {
        String record = new String(rec);
        String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                            RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE).replace("\0", "");
        if (BN_NAME.toLowerCase().equals(input.toLowerCase())) {
           System.out.println(record.substring(RID_SIZE, RID_SIZE + REGISTER_NAME_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE,
                                                RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + BN_REG_DT_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + BN_REG_DT_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE +
                                                BN_STATUS_SIZE + BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + 
                           BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE +
                                                BN_STATUS_SIZE + BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + 
                           BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE +
                                                BN_STATUS_SIZE + BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE +
                                                BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + BN_REG_DT_SIZE +
                           BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE + BN_STATE_OF_REG_SIZE));
           System.out.println(record.substring(RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE +
                                                BN_STATUS_SIZE + BN_REG_DT_SIZE + BN_CANCEL_DT_SIZE +
                                                BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE + BN_STATE_OF_REG_SIZE,
                   RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE + BN_STATUS_SIZE + BN_REG_DT_SIZE +
                           BN_CANCEL_DT_SIZE + BN_RENEW_DT_SIZE + BN_STATE_NUM_SIZE + BN_STATE_OF_REG_SIZE + BN_ABN_SIZE));
           return true;
        } else {
            return false;
        }
    }
}
