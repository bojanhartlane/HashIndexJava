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
            Long.parseLong(s);
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
        int recordsInPage = pagesize / INDEX_SIZE;
        System.out.println("Number of records in one page: " + recordsInPage);        
        
        try
        {
            FileInputStream fisHash = new FileInputStream(hashfile);
            FileInputStream fisHeap = new FileInputStream(heapfile);
            boolean foundRecord = false;
            
            int queryHashCode = Math.abs(query.toLowerCase().hashCode());
            System.out.println("Hashcode: " + queryHashCode);
            int currentBucket = queryHashCode % totalBuckets;
            System.out.println("Bucket: " + currentBucket);
            int pointer = 0;
            long skipPointer = (long)(currentBucket * pagesize);
            
            while (!foundRecord) {
                byte[] bPage3 = new byte[pagesize];
                fisHash.skip(skipPointer);
                fisHash.read(bPage3, 0, INDEX_SIZE);
                System.out.println(new String(bPage3));
                foundRecord = true;
            }
            
            // TESTING
            byte[] bPage2 = new byte[pagesize];
            // TESTING
            fisHeap.skip(4096);
            fisHeap.read(bPage2, 0, RECORD_SIZE);            
            // TESTING
            fisHeap.skip(-(4096 + RECORD_SIZE));
            fisHeap.read(bPage2, 0, RECORD_SIZE);
            // TESTING
            fisHeap.skip(-(0 + RECORD_SIZE));
            fisHeap.skip(297);
            fisHeap.read(bPage2, 0, RECORD_SIZE);
            // TESTING		 
            fisHeap.skip(-(297 + RECORD_SIZE));
            fisHeap.read(bPage2, 0, RECORD_SIZE);            
            // TESTING
            fisHeap.skip(-(0 + RECORD_SIZE));
            
            
            isNextPage = false;
            // reading page by page
            while (isNextPage)
            {
                byte[] bPage = new byte[pagesize];
                byte[] bPageNum = new byte[intSize];
                fisHeap.read(bPage, 0, pagesize);
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
                    // TESTING
                    FileOutputStream fos = new FileOutputStream(hashfile);
                    fos.close();
                }
                pageCount++;		
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
    public int hashRecord(byte[] rec, int totalBuckets)
    {
        String record = new String(rec);
        String BN_NAME = record.substring(RID_SIZE+REGISTER_NAME_SIZE,
                                            RID_SIZE+REGISTER_NAME_SIZE+BN_NAME_SIZE);
        int bucketNum = Math.abs(BN_NAME.toLowerCase().hashCode()) % totalBuckets;      
        return bucketNum;
    }   
}
