package wordcountservice;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@WebListener
public class WordCountServiceInitializer implements ServletContextListener
{
	private ServletContext context = null;

	private class WordCountFetch implements Runnable 
    {

		private String fname = null;
        private CountDownLatch fetchCount = null;

        /**
         * constructor for Wordcount fetch thread
         *
         * @param fname: the file to read data
         * @param fetchCount: job counter, help exectuorService to monitor whether a job is finished
         */
        public WordCountFetch(String fname, CountDownLatch fetchCount)
        {
          this.fname = fname;
          this.fetchCount = fetchCount;
        }
        /**
         * thread start function
         */
        @Override
        public void run()
        {
        	File infile = new File(fname);
        	Scanner in = null;
        	try
        	{
        		try
        		{
        			in = new Scanner(infile);
        			while (in.hasNext())
        			{	
        				updateMap(in.next());
        			}
        		}
        		finally{
        			in.close();
        			fetchCount.countDown();
        		}
        	}
            catch(FileNotFoundException e)
            {
            	log("ContextListener: unfound file:"+fname);
            }
        }
    }

    /**
     * increase query_count for corresponding key
     *
     * @param key: look-up key
     */
	private void updateMap(String key)
	{
		//wordCountMap store <word , query_countxword_count>
		HashMap<String, String> wordCountMap = null;
		try
		{
			wordCountMap = Functions.getHashMap(key, context);
		}
		catch(NoSuchAlgorithmException e)
    	{
    		log("unknow algorithm for modulo");
    	}
		
		synchronized (wordCountMap)
		{
			if (wordCountMap.containsKey(key)){
				String [] vals = wordCountMap.get(key).split("x");
				
	    		StringBuilder sb = new StringBuilder();
	    		
	    		int carryin = 1;
	    		boolean allNight = true;
	    		for (int i = vals[1].length()-1 ; i>=0; i--)
	    		{
	    			int sum = (vals[1].charAt(i)-'0' + carryin);
	    			allNight &= (vals[1].charAt(i)-'0' == 9);
	    			carryin = sum/10;
	    			sb.append(sum%10);
	    		}
	    		if (allNight)
	    			sb.append(carryin);
	    		
	    		sb.reverse();
	    		wordCountMap.put(key,"0x"+sb.toString());
	    		
			}else{
				wordCountMap.put(key,"0x1");
			}			
		}
	}	

    /**
     * initialize servlet context 
     *
     * @param servelet context event
     */
    public void contextInitialized(ServletContextEvent sce)
    {
        // prepare file for file read input
    	log("context initialization...");

    	//prepare md5 hash digest for hashmap fetching
    	context = sce.getServletContext();
    	try
    	{
    		context.setAttribute("messageDigest", MessageDigest.getInstance("MD5"));
    	}
    	catch(NoSuchAlgorithmException e)
    	{
    		log("unknow algorithm for modulo");
    	}

        //multi-thread processing  word-count related txts
    	CountDownLatch wordCountLatch = null;
        ExecutorService executor = Executors.newFixedThreadPool(20);
        String webAppPath = this.getClass().getClassLoader().
        						getResource("/").getPath();
        File directory = new File(webAppPath+"../../txtfiles");

        if (directory.exists() && directory.isDirectory())
        {
        	String [] fnames = directory.list();
        	wordCountLatch = new CountDownLatch(fnames.length);
        	for (String fname : fnames)
        		executor.execute(new WordCountFetch(
        				directory.getAbsolutePath()+"/"+fname,
        				wordCountLatch
        				));
        }

        executor.shutdown();

        //wait for all processing jobs done
        try
        {
            if (wordCountLatch!= null)
                wordCountLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log("context initialization done");
    }

    /**
     * destroy servlet context 
     *
     * @param servelet context event
     */
    public void contextDestroyed(ServletContextEvent sce)
    {
    }

    /**
     * Log a message to the servlet context application log.
     *
     * @param message Message to be logged
     */
    private synchronized void log(String message)
    {
    	if (context != null)
    	    context.log("ContextListener: " + message);
    	else
    	{
    	    System.out.println("ContextListener: " + message);
        }
	}

}
