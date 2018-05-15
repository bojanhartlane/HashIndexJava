import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 *  Database Systems - HASHED INDEXES IMPLEMENTATION
 */

public class hashload implements dbimpl
{
    // initialize
    public static void main(String args[])
    {
        hashload load = new hashload();
        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }


    // reading command line arguments
    public void readArguments(String args[])
    {
        if (args.length == 1) {
            if (isInteger(args[0])) {
               writeIndex(Integer.parseInt(args[0]));
            }
        }
        else {
            System.out.println("Error: only pass in page size");
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

   // read heapfile as a whole and write the location of each record to "hash.<pagesize>" file
   public void writeIndex(int pagesize)
   {
        File heapfile = new File(HEAP_FNAME + pagesize);
        File hashfile = new File(HASH_FNAME + pagesize);
        int intSize = 4;
        int pageCount = 0;
        int recCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextPage = true;
        boolean isNextRecord = true;
        
        //Buckets variables
        int totalBuckets = (MAXIMUM_RECORDS * SLOT_SIZE / pagesize);
        int modBucket = (MAXIMUM_RECORDS * SLOT_SIZE) % pagesize;
        if (modBucket > 0) {
            totalBuckets++;
        }
        System.out.println("Total buckets: " + totalBuckets);
        // Maximum number of slots in a bucket
        int slotsInBucket = pagesize / SLOT_SIZE;
        System.out.println("Number of records in one bucket: " + slotsInBucket);
        // Array of indexes that will be loaded into "hash.<pagesize>" file
        int[][] indexes = new int[totalBuckets][slotsInBucket];
        for (int i = 0; i < totalBuckets; i++) {
            for (int j = 0; j < slotsInBucket; j++) {
                indexes[i][j] = -1;
            }
        }
        
        try
        {
            FileInputStream fisHeap = new FileInputStream(heapfile);            
            // Reading one page at a time
            while (isNextPage)
            {
                byte[] bPage = new byte[pagesize];
                byte[] bPageNum = new byte[intSize];
                fisHeap.read(bPage, 0, pagesize);
                System.arraycopy(bPage, bPage.length-intSize, bPageNum, 0, intSize);			

                // Reading by record, return true to read the next record
                isNextRecord = true;
                while (isNextRecord)
                {
                    byte[] bRecord = new byte[RECORD_SIZE];
                    byte[] bRid = new byte[intSize];
                    try
                    {
                        System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                        System.arraycopy(bRecord, 0, bRid, 0, intSize);
                        rid = ByteBuffer.wrap(bRid).getInt();				  
                        if (rid != recCount) {
                           isNextRecord = false;
                        }
                        else {
                            // Bucket index to insert record position
                            int bucketIndex = hashRecord(bRecord, totalBuckets);
                            // Position of the current slot in the bucket
                            int bucketIndexPosition = 0;
                            boolean insertRecord = false;
                            while (!insertRecord) {
                                // If a slot in bucket is empty, then insert record location to the slot
                                if (indexes[bucketIndex][bucketIndexPosition] == -1) {
                                    indexes[bucketIndex][bucketIndexPosition] = (pageCount * pagesize + rid * RECORD_SIZE);
                                    insertRecord = true;
                                }
                                // If there is no empty slot in a bucket, look for empty slot in the next bucket
                                else if (bucketIndexPosition == (slotsInBucket - 1)) {
                                    // If there is still no empty slot in the remaining buckets,
                                    // start looking from the first bucket
                                    if (bucketIndex == (totalBuckets - 1)) {
                                        bucketIndex = 0;
                                    } else {
                                        bucketIndex++;
                                    }
                                    bucketIndexPosition = 0;
                                } else {
                                    bucketIndexPosition++;
                                }
                            }                            
                            
                            recordLen += RECORD_SIZE;
                        }
                        recCount++;
                        // if recordLen exceeds pagesize, catch this to reset to next page
                    }
                    catch (ArrayIndexOutOfBoundsException e)
                    {
                        isNextRecord = false;
                        recordLen = 0;
                        recCount = 0;
                        rid = 0;
                    }
                }
                // Check if all pages have been scanned
                if (ByteBuffer.wrap(bPageNum).getInt() != pageCount)
                {
                    isNextPage = false;
                    // Write indexes to "hash.<pagesize>" file
                    DataOutputStream dosHash = new DataOutputStream(new FileOutputStream(hashfile));
                    for (int i = 0; i < totalBuckets; i++) {
                        for (int j = 0; j < slotsInBucket; j++) {
                            dosHash.writeInt(indexes[i][j]);
                        }
                        //Add padding at the end of each page
                        byte[] fPadding = new byte[pagesize - (slotsInBucket * SLOT_SIZE)];
                        dosHash.write(fPadding);
                    }
                    dosHash.close();
                    fisHeap.close();
                }
                pageCount++;		
            }
        }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
      }
      catch (IOException e)
      {
         e.printStackTrace();
      }
   }

   // Returns bucket index for a particular record
   public int hashRecord(byte[] rec, int totalBuckets)
   {
      String record = new String(rec);
      // Get the BN_NAME field only and remove null characters from BN_NAME field
      String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                        RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE).replace("\0", "");
      // Calculate bucket index and return the result
      int bucketNum = Math.abs(BN_NAME.toLowerCase().hashCode()) % totalBuckets;      
      return bucketNum;
   }   
}
