package org.ruppyrup.basicrdds;

import java.util.Set;

/**
 * A wrapper for an input file containing a list of what we think are "boring" words.
 * 
 * The list was generated by running a word count across all of VirtualPairProgrammer's subtitle files.
 * 
 * Words that appear in every single course must (we think) be "boring" - ie they don't have a relevance
 * to just one specific course.
 * 
 * This list of words is "small data" - ie it can be safely loaded into the driver's JVM - no need to 
 * distribute this data.
 */
public class BoringUtilities
{
	public static Set<String> borings = Set.of(
			"to",
			"from",
			"it",
			"there",
			"tidy",
			"a",
			"and",
			"the",
			"of",
			"we",
			"this",
			"is",
			"i",
			"on",
			"for",
			"be",
			"do",
			"you",
			"in",
			"so",
			"if",
			"but",
			"can",
			"that",
			"have",
			"going"
	);

//	static {
//		// On the video, I say "change this class name" - ignore that.
//		// The class name should have been Util, as it is here!
//		InputStream is = BoringUtilities.class.getResourceAsStream("src/main/resources/data/subtitles/boringwords.txt");
//		BufferedReader br = new BufferedReader(new InputStreamReader(is));
//		br.lines().forEach(borings::add);
//	}

	/**
	 * Returns true if we think the word is "boring" - ie it doesn't seem to be a keyword
	 * for a training course.
	 */
	public static boolean isBoring(String word)
	{
		return borings.contains(word);
	}

	/**
	 * Convenience method for more readable client code
	 */
	public static boolean isNotBoring(String word)
	{
		return !isBoring(word);
	}

	public static void main(String[] args) {
		borings.forEach(System.out::println);
	}
	
}
