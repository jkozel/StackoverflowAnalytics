package honza;

import java.util.Arrays;
import java.util.List;

/**
 * Extracts data from the Stackoverflow dump.
 * <p>
 * The Stockoverflow dump is as a series of XML nodes (not as a well-formed XML document) where each entry is one line, in the following format:
 * <pre>
<row 
Id="24665723" 
PostTypeId="1" 
AcceptedAnswerId="24665772" 
CreationDate="2014-07-10T00:25:26.670" 
Score="0" 
ViewCount="27" 
Body="&lt;p&gt;I am getting data in jquery as below , I want to convert to one dimensional string Array&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;[&#xA;    {&quot;2065559857&quot;:&quot;2065559482&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065553412&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065558122&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155354848&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155577723&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7153555598&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065551189&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155544434&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7296363080&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7890128703&quot;},&#xA;    {&quot;2065559857&quot;:&quot;8483894326&quot;},&#xA;    {&quot;2065559857&quot;:&quot;9077659950&quot;},&#xA;    {&quot;2065559857&quot;:&quot;9671425573&quot;}&#xA;]&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;convert into &lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;[&quot;2065559482&quot;,&quot;2065559857&quot;,&quot;2065553412&quot;,.....]&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;" 
OwnerUserId="1961552" LastEditorUserId="2518525" LastEditDate="2014-07-10T00:30:30.167" LastActivityDate="2014-07-10T00:34:04.423" 
Title="jquery to conert object data into string array" 
Tags="&lt;jquery&gt;&lt;arrays&gt;&lt;json&gt;" 
AnswerCount="2" 
CommentCount="2" />
 * </pre>
 *
 * @author jkozel
 */
public class StackoverflowParser {
	private static final String CREATION_DATE_PROP_NAME = "CreationDate";
	private static final String TAGS_PROP_NAME = "Tags"; 
	private static final int monthFormatLength = "yyyy-MM".length();

	private String extractPropertyValue(String propertyName, String input) {
		String propertyOpening = propertyName + "=\"";
		int iStart = input.indexOf(propertyOpening);
		if (iStart >= 0) {
			iStart += propertyOpening.length();
			int iEnd = input.indexOf("\"", iStart);
			if (iEnd > iStart) {
				String value = input.substring(iStart, iEnd);
				return value;
			} else {
				throw new IllegalArgumentException("The property name \"" + propertyName + "\" is missing the closing double-quote for the input String: " + input);
			}
		} else {
			throw new IllegalArgumentException("The property name \"" + propertyName + "\" is missing from the input String: " + input);
		}
	}

	/**
	 * Extract the String representing the year and month (in the format "yyyy-MM") in which the post was created.
	 * @param inStr
	 * @return
	 * @throws IllegalArgumentException
	 */
	public String extractCreationMonthStr(String input) {
		String propValue = extractPropertyValue(CREATION_DATE_PROP_NAME, input);
		if (propValue != null && propValue.length() > monthFormatLength) {
			return propValue.substring(0, monthFormatLength);
		} else {
			throw new IllegalArgumentException("The value for the property \"" + CREATION_DATE_PROP_NAME + "\" is too short. Must start as \"yyyy-MM\". Found: \"" + propValue + "\"");
		}
	}

	/**
	 * Extract the list of tags for the post.
	 * @param inStr
	 * @return
	 */
	public List<String> extractTags(String input) {
		String value = extractPropertyValue(TAGS_PROP_NAME, input);
		String cleanValue = value.replaceAll("&gt;&lt;", "|").replaceAll("&lt;", "").replaceAll("&gt;", "");
		String[] split = cleanValue.split("\\|");
		return Arrays.asList(split);
	}
}
