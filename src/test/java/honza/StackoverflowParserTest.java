package honza;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import honza.StackoverflowParser;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class StackoverflowParserTest {
	private static final String SAMPLE_ROW = "  <row Id=\"24665723\" PostTypeId=\"1\" AcceptedAnswerId=\"24665772\" CreationDate=\"2014-07-10T00:25:26.670\" Score=\"0\" ViewCount=\"27\" "
			+ "Body=\"&lt;p&gt;I am getting data in jquery as below , I want to convert to one dimensional string Array&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;[&#xA;    {&quot;2065559857&quot;:&quot;2065559482&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065553412&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065558122&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155354848&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155577723&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7153555598&quot;},&#xA;    {&quot;2065559857&quot;:&quot;2065551189&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7155544434&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7296363080&quot;},&#xA;    {&quot;2065559857&quot;:&quot;7890128703&quot;},&#xA;    {&quot;2065559857&quot;:&quot;8483894326&quot;},&#xA;    {&quot;2065559857&quot;:&quot;9077659950&quot;},&#xA;    {&quot;2065559857&quot;:&quot;9671425573&quot;}&#xA;]&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;convert into &lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;[&quot;2065559482&quot;,&quot;2065559857&quot;,&quot;2065553412&quot;,.....]&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;\" "
			+ "OwnerUserId=\"1961552\" LastEditorUserId=\"2518525\" LastEditDate=\"2014-07-10T00:30:30.167\" LastActivityDate=\"2014-07-10T00:34:04.423\" "
			+ "Title=\"jquery to conert object data into string array\" "
			+ "Tags=\"&lt;jquery&gt;&lt;arrays&gt;&lt;json&gt;\" "
			+ "AnswerCount=\"2\" CommentCount=\"2\" />";
	private static final String ROW_MISSING_PROPERTIES = "  <row Id=\"24665723\" />";

	private StackoverflowParser parser = new StackoverflowParser();

	@Test
	public void testExtractCreationMonthStr() {
		String month = parser.extractCreationMonthStr(SAMPLE_ROW);
		assertEquals("2014-07", month);
		try {
			month = parser.extractCreationMonthStr(ROW_MISSING_PROPERTIES);
			fail("Missing property should have thrown an IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// Expected
		}
	}

	@Test
	public void testExtractTags() {
		List<String> tags = parser.extractTags(SAMPLE_ROW);
		assertEquals(Arrays.asList("jquery", "arrays", "json"), tags);
		try {
			tags = parser.extractTags(ROW_MISSING_PROPERTIES);
			fail("Missing property should have thrown an IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// Expected
		}
	}
}
