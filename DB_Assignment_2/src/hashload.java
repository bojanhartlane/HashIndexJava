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
        int totalBuckets = (MAXIMUM_RECORDS * INDEX_SIZE / pagesize);
        int modBucket = (MAXIMUM_RECORDS * INDEX_SIZE) % pagesize;
        if (modBucket > 0) {
            totalBuckets++;
        }
        System.out.println("Total buckets: " + totalBuckets);
        int recordsInPage = pagesize / INDEX_SIZE;
        System.out.println("Number of records in one page: " + recordsInPage);
        // Array of indexes that will be loaded into "hash.(pagesize)" file
        int[][] indexes = new int[totalBuckets][recordsInPage];
        for (int i = 0; i < totalBuckets; i++) {
            for (int j = 0; j < recordsInPage; j++) {
                indexes[i][j] = -1;
            }
        }
        
        try
        {
            FileInputStream fis = new FileInputStream(heapfile);            
            // reading page by page
            while (isNextPage)
            {
                byte[] bPage = new byte[pagesize];
                byte[] bPageNum = new byte[intSize];
                fis.read(bPage, 0, pagesize);
                System.arraycopy(bPage, bPage.length-intSize, bPageNum, 0, intSize);			

                // reading by record, return true to read the next record
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
                            int bucketIndexPosition = 0;
                            boolean insertRecord = false;
                            while (!insertRecord) {
                                if (indexes[bucketIndex][bucketIndexPosition] == -1) {
                                    indexes[bucketIndex][bucketIndexPosition] = (pageCount * pagesize + rid * RECORD_SIZE);
                                    insertRecord = true;
                                } else if (bucketIndexPosition == (recordsInPage - 1)) {
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
                // check to complete all pages
                if (ByteBuffer.wrap(bPageNum).getInt() != pageCount)
                {
                    isNextPage = false;
                    // Write indexes to the hash file
                    FileOutputStream fos = new FileOutputStream(hashfile);                    
                    for (int i = 0; i < totalBuckets; i++) {
                        for (int j = 0; j < recordsInPage; j++) {
                            byte[] DATA = new byte[INDEX_SIZE];
                            String entry = String.valueOf(indexes[i][j]);
                            byte[] DATA_SRC = entry.trim().getBytes(ENCODING);
                            System.arraycopy(DATA_SRC, 0, DATA, 0, DATA_SRC.length);
                            fos.write(DATA);
                        }
                        //Add padding at the end of the page
                        byte[] fPadding = new byte[pagesize - (recordsInPage * INDEX_SIZE)];
                        fos.write(fPadding);
                    }
                    fos.close();
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

   // returns bucket index for a particular record
   public int hashRecord(byte[] rec, int totalBuckets)
   {
      String record = new String(rec);
      String BN_NAME_SUBSTRING = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                            RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);
      // Remove null characters before calculating the hash code
      String BN_NAME = BN_NAME_SUBSTRING.replace("\0", "");
      int bucketNum = Math.abs(BN_NAME.toLowerCase().hashCode()) % totalBuckets;      
      return bucketNum;
   }   
}
