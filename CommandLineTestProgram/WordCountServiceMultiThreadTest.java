import java.io.*;
import java.util.*;
import java.util.Random;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;

import java.net.URL; 
import java.net.URLConnection; 


import java.net.MalformedURLException;
import java.io.IOException;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
* WordCountServiceMultiThreadTest:
*   to test WordcountServiceEndPoint Using MultiThread Behavior
*/
public class WordCountServiceMultiThreadTest {

    /**
     * file logger
     *    to store multi-thread querying result in file
     *    one line will be one query result
     *    the format will be queried_word, queried_count, word_count
     *    using lazy initilization and singleon pattern
     */
    public static class FileLogger
    {
        private static FileLogger inst = null;
        private BufferedOutputStream bos = null;

        /**
         * constructor FileLogger
         *
         * @param fname: the file to output queried data
         */
        private FileLogger(String fname)
        {
            super();
            try
            {
                File file = new File(fname);
                if (file.exists())
                    file.delete();
                file.createNewFile();
                bos = new BufferedOutputStream(new FileOutputStream(file, true));
            }
            catch(FileNotFoundException e)
            {
                System.out.println("unfound file:"+fname);
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }

        /**
         * write chunk byte to output file
         *
         * @param chunk : byte array data
         * @param count : available count of byte array
         */
        public synchronized void write(byte[] chunk, int count) throws IOException
        {
            bos.write(chunk, 0, count);
            bos.flush();
        }

        /**
         * generate fileLogger instance
         *
         * @param fname: the file to output queried data
         */
        public synchronized static FileLogger getFileLogger(String fname)
        {
            if (inst == null)
                inst = new FileLogger(fname);
            return inst;
        }
    }

    /**
    * WordCountfecth:
    *   a runnable job to read word-count related txts for result comparision
    */
    public static class WordCountFetch implements Runnable 
    {

        private String fname = null;
        // store <word , word_count>
        private HashMap<String, String> wordCountMap = null;
        private CountDownLatch fetchCount = null;

        /**
         * constructor WordCountFetch
         *
         * @param fname: the file to read data
         * @param wordCountMap: to store data as <word, word_count>
         * @param fetchCount: job counter, help exectuorService to monitor whether a job is finished
         */
        public WordCountFetch(String fname, HashMap<String, String> wordCountMap,
            CountDownLatch fetchCount)
        {
          this.fname = fname;
          this.wordCountMap = wordCountMap;
          this.fetchCount = fetchCount;
        }

        /**
         * thread start function
         */
        @Override
        public void run()
        {
            //System.out.println(fname);
            File infile = new File(fname);
            Scanner in = null;
            try
            {
                try
                {
                    in = new Scanner(infile);
                    while (in.hasNext())
                    {   
                        updateMap(in.next(), wordCountMap);
                    }
                }
                finally{
                    in.close();
                    fetchCount.countDown();
                }
            }
            catch(FileNotFoundException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
    * QueryFetch:
    *   a runnable job to query restful service 
    */
    public static class QueryFetch implements Runnable 
    {
        // buffering size
        private static int CHUNKSIZE = 4096;
        // restful serivce URI
        private static final String DOMAINURI = "http://localhost:8080/WordCountService/wordcountservice/query?word=";

        private String queryWord = null;
        private FileLogger fileLogger = null;
        private CountDownLatch fetchCount = null;

        /**
         * constructor QueryFetch
         *
         * @param queryWord: the word to query restful service.
         * @param fileLogger: file logger to to store data as <word query_count word_count>
         * @param fetchCount: job counter, help exectuorService to monitor whether a job is finished
         */
        public QueryFetch(String queryWord, FileLogger fileLogger,
            CountDownLatch fetchCount)
        {
          this.queryWord = queryWord;
          this.fileLogger = fileLogger;
          this.fetchCount = fetchCount;
        }

        /**
         * thread start function
         */
        @Override
        public void run()
        {

            byte[] chunk = new byte[CHUNKSIZE];
            int count;
            try
            {    
                URL pageUrl = new URL(DOMAINURI+queryWord);

                BufferedInputStream bis = new BufferedInputStream(pageUrl.openStream());
                try{
                    while ((count = bis.read(chunk, 0, CHUNKSIZE)) != -1)
                    {
                        fileLogger.write(chunk, count);
                    }
                }
                finally
                {
                    bis.close();
                    fetchCount.countDown();
                }                 
            }
            catch(MalformedURLException e)
            {
                System.out.println("URL malform exception:"+DOMAINURI+queryWord);   
            }
            catch (IOException e)
            { 
                e.printStackTrace();
            }
        }
    }

    /**
     * update corresponding word count for each word
     *
     * @param key: the word whose word count will be updated
     * @param wordCountMap: to store data as <word, word_count>
     */
    public synchronized static void updateMap(String key, HashMap<String, String> wordCountMap)
    {
        if (wordCountMap.containsKey(key)){
            String val = wordCountMap.get(key);
            
            StringBuilder sb = new StringBuilder();
            
            int carryin = 1;
            boolean allNine = true;
            for (int i = val.length()-1 ; i>=0; i--)
            {
                int sum = (val.charAt(i)-'0' + carryin);
                allNine &= (val.charAt(i)-'0' == 9);
                carryin = sum/10;
                sb.append(sum%10);
            }
            if (allNine)
                sb.append(carryin);
            
            wordCountMap.put(key, sb.reverse().toString());
            
        }else{
            wordCountMap.put(key,"1");
        }
    }

    /**
     * QueryKeyMisMatchException
     *  user-define exception to indicate that restful service's query key is 
     *  not consistent with txtfiles provided
     */
    public static class QueryKeyMisMatchException extends Exception
    {
        /**
         * constructor QueryKeyMisMatchException
         *
         * @param message: to indicate possible error
         */
        public QueryKeyMisMatchException(String message)
        {
            super(message);
        }
    }

    /**
     * QueryCountDupException
     *  user-define exception to indicate that restful service's query count has
     *  duplication for multi thread querying
     */
    public static class QueryCountDupException extends Exception
    {
        /**
         * constructor QueryCountDupException
         *
         * @param message: to indicate possible error
         */
        public QueryCountDupException(String message)
        {
            super(message);
        }
    }

    /**
     * QueryWordCountMisMatchException
     *  user-define exception to indicate that restful service's word count is 
     *  not consistent with txtfiles provided
     */
    public static class QueryWordCountMisMatchException extends Exception
    {
        /**
         * constructor QueryWordCountMisMatchException
         *
         * @param message: to indicate possible error
         */
        public QueryWordCountMisMatchException(String message)
        {
            super(message);
        }
    }
    /**
     * QueryCountMissException
     *  user-define exception to indicate that restful service's query count has
     *  missing for multi thread querying
     */
    public static class QueryCountMissException extends Exception
    {
        /**
         * constructor QueryCountMissException
         *
         * @param message: to indicate possible error
         */
        public QueryCountMissException(String message)
        {
            super(message);
        }
    }
    /**
     * NonValidDirectoryException
     *  user-define exception to indicate that provided -d is not directory
     */
    public static class NonValidDirectoryException extends Exception
    {
        /**
         * constructor NonValidDirectoryException
         *
         * @param message: to indicate possible error
         */
        public NonValidDirectoryException(String message)
        {
            super(message);
        }
    }

    /**
     * Compre txtfiles provide with resutful service multi thread querying result
     * necessary exception will be threw out for indication
     * @param filename: the file to store queryed result as [word query_count word_count]
     * @param wordCountMap: to store data as <word, word_count> provided by txtfiles
     * @param queryStr: the word to query restful serivce
     * @param numOfReq: num of queries performed by multi threading
     */
    private  static void verifyResult(String fileName, HashMap<String, String> wordCountMap,
        String queryStr, int numOfReq)
        throws QueryKeyMisMatchException, QueryCountDupException,
        QueryWordCountMisMatchException, QueryCountMissException
    {

        File infile = new File(fileName);
        Scanner in = null;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        HashSet<Integer> queryCounts = new HashSet<Integer>(); 
        try
        {
            try
            {
                in = new Scanner(infile);

                while (in.hasNext())
                {
                    // query str
                    if (in.hasNext())
                    {
                        String str =  in.next();
                        if (!str.equals(queryStr))
                            throw new QueryKeyMisMatchException("unmatched query word: "+queryStr+ " v.s "+str);
                    }
                    // query count
                    if (in.hasNext())
                    {
                        //assume int is enough to store possible numofReg
                        int queryCount = Integer.parseInt(in.next());
                        min = Math.min(queryCount, min);
                        max = Math.max(queryCount, max);
                        if (queryCounts.contains(queryCount))
                            throw new QueryCountDupException("queryCount duplication: "+queryCount);
                        queryCounts.add(queryCount);
                    }

                    // word count
                    if (in.hasNext()){
                        String str =  in.next();
                        String tarCount = wordCountMap.containsKey(queryStr) ? wordCountMap.get(queryStr) : "0";
                        if (!str.equals(tarCount))
                            throw new QueryWordCountMisMatchException("unmatched query count: "+tarCount+" v.s "+str);
                    }
                }
                
            }
            finally{
                in.close();
            }
        }
        catch(FileNotFoundException e)
        {
            System.out.println("unfound file: "+fileName);
        }

        if (queryCounts.size() != numOfReq || max-min+1 != numOfReq)
            throw new QueryCountMissException("numOfReq miss certain of requests: "+numOfReq);
    }
    
    
    public static void main(String[] args)  throws Exception {

        System.out.println("test initizalize and setup...");

        //paring arguments to get
        //-f: output file
        //-t: query count 
        //-w: query word 
        //-d: the directory of txtfiles
        String fileName = null;
        String wordsFileDirectory = null;
        String queryWord = null;
        String queryTime = null;
        for (int i = 0; i < args.length; i++)
        {
            switch (args[i].charAt(0))
            {
                case '-':
                    if (args[i].length() !=2)
                        throw new IllegalArgumentException("Not a valid argument: "+args[i]);

                    if (args.length-1 == i)
                        throw new IllegalArgumentException("Expected arg after: "+args[i]);

                    switch (args[i].charAt(1))
                    {
                        case 'f':
                            fileName = args[i+1];
                            break;
                        case 'd':
                            wordsFileDirectory = args[i+1];
                            break;
                        case 'w':
                            queryWord = args[i+1];
                            break;    
                        case 't':
                            for (int k = 0 ; k < args[i+1].length() ; k++)
                            {
                                if (!(args[i+1].charAt(k) >= '0' && args[i+1].charAt(k) <='9'))
                                    throw new IllegalArgumentException("Not a valid option for time: "+args[i+1]);
                            }
                            queryTime = args[i+1];
                            break;
                        default:
                            throw new IllegalArgumentException("Not a valid option: -"+args[i+1]);
                    }
                    i++;
                    break;
                default:
                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
            }
        }

        if (fileName == null)
            throw new IllegalArgumentException("missing ouput file name -f");

        if (queryWord == null)
            throw new IllegalArgumentException("missing queryword -w");

        if (queryTime == null)
            throw new IllegalArgumentException("missing queryTime -t");

        File directory = new File(wordsFileDirectory);
        if (!directory.isDirectory())
            throw new NonValidDirectoryException("Not a directory:"+directory.getAbsolutePath());

        if (!(directory.exists()))
            throw new FileNotFoundException("directory does not exist:"+directory.getAbsolutePath());
        
        //prepare thread pool
        ExecutorService executor = Executors.newFixedThreadPool(20);

        //multi-thread processing word-count related txts
        CountDownLatch wordCountLatch = null;
        HashMap<String, String> wordCountMap = new HashMap<String, String>();
        if (directory.exists() && directory.isDirectory()){
            String [] filesUnderDirec = directory.list();
            wordCountLatch = new CountDownLatch(filesUnderDirec.length);
            for (String fileUnderDirec : filesUnderDirec)
            {
                executor.execute(new WordCountFetch(
                    directory.getAbsolutePath()+"/"+fileUnderDirec, wordCountMap, wordCountLatch));
            }
        }

        ///multi-thread Restful service query processing accordung  to querytime
        int numOfReq = Integer.parseInt(queryTime);
        CountDownLatch queryCountLatch = new CountDownLatch(numOfReq);
        FileLogger fileLogger = FileLogger.getFileLogger(fileName);
        for (int i = 0 ; i < numOfReq; i++)
        {
            executor.execute(new QueryFetch(queryWord, fileLogger, queryCountLatch));
        }

        //wait for all queued jobs to executor service finished
        try
        {
            if (wordCountLatch!= null)
                wordCountLatch.await();
            queryCountLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //shutdown executor service
        System.out.println("all preprocessing and word restful fetch done!!");
        executor.shutdown();

        //verfiy queried result against txtfiles provided
        System.out.println("verfiy expected result and restufl fetch result...");
        verifyResult(fileName, wordCountMap, queryWord, numOfReq);

        System.out.println("verify result success!!");
    }
}