package wordcountservice;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(urlPatterns={"/wordcountservice/query"})
@SuppressWarnings("serial")
public class WordCountServiceEndPoint extends HttpServlet 
{
    /**
     * WordCountServiceEndPoint doGet endpoint
     *
     * @param req: ServeletRequest object
     * @param resp: ServeletResponse object
     */
	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                        throws ServletException, IOException 
    {
    	getServletContext().log(req.getRequestURL()+"?"+req.getQueryString());
    	
        PrintWriter out = resp.getWriter();

        resp.setHeader("Content-Type", "text/plain");

        String key = (String) req.getParameter("word");
        String [] values = null;
 
        // fetch hashmap based on key
		HashMap<String, String> wordCountMap = null;
		try
		{	
			wordCountMap = Functions.getHashMap(key, getServletContext());
		}
		catch(NoSuchAlgorithmException e)
    	{
    		log("unknow algorithm for modulo");
    	}
        
     
        synchronized (wordCountMap) 
        {
        	// fetch queried key's word count and increase query_count 
        	String value = wordCountMap.get(key);
        	if (value != null)
        	{
        		//formatter: Query_countxWord_count  
        		values = value.split("x");
        		
        		StringBuilder sb = new StringBuilder();
        		int carryin = 1;
        		boolean allNine = true;
        		for (int i = values[0].length()-1 ; i>=0; i--)
        		{
        			int sum = (values[0].charAt(i)-'0' + carryin);
        			allNine &= (values[0].charAt(i)-'0' == 9);
        			carryin = sum/10;
        			sb.append(sum%10);
        		}
        		if (allNine)
        			sb.append(carryin);
        		
        		values[0] = sb.reverse().toString();
        		sb.append('x');
        		sb.append(values[1]);
        		wordCountMap.put(key, sb.toString());
        	}else{
        		wordCountMap.put(key,"1x0");
        		values = new String[]{"1", "0"};
        	}

        }

        //output format, [query_string query_count word_count]
        out.print(key);
        out.print(" ");
        out.print(values[0]);
        out.print(" ");
        out.println(values[1]);

        out.close();
    }
}
