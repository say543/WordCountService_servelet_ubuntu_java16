package wordcountservice;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import javax.servlet.ServletContext;

public class Functions {
    /**
     * generate hash value according input string and digest algorithm 
     *
     * @param input: string to hash
     * @param md: message-digesting algorithm object 
     */
	private static String getModulo(String input, MessageDigest md) throws NoSuchAlgorithmException
	{
		//System.out.println(input);
		String hashtext = null;

		byte[] messageDigest = md.digest(input.getBytes());
		BigInteger number = new BigInteger(1, messageDigest);
		hashtext = number.toString(16);

		while (hashtext.length() < 32)
		{
			hashtext = "0" + hashtext;
		}
		
		return hashtext;
    }

    /**
     * retrieve hashMap based on input string's digested hash value
     *
     * @param input: string to hash
     * @param context: ServeletContext, storing message-digesting algorithm object 
     */
	@SuppressWarnings("unchecked")
	public synchronized static HashMap<String , String > getHashMap(String key, ServletContext context) throws NoSuchAlgorithmException
	{
		HashMap<String, String> wordCountMap = null;
		
		String digestKey = Functions.getModulo(key, (MessageDigest) context.getAttribute("messageDigest"));

		wordCountMap = (HashMap<String, String>) context.getAttribute(digestKey);
		if (wordCountMap == null)
		{
			wordCountMap = new HashMap<String, String>();
			context.setAttribute(digestKey, wordCountMap);
		}
		return wordCountMap;
	}
}
