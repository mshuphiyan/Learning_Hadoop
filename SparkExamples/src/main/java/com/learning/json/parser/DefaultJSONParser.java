package com.learning.json.parser;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class DefaultJSONParser {

	public static void main(String[] args) throws Exception {
		JSONParser parser = new JSONParser();

		Object obj = parser.parse(new FileReader(
				"/Volumes/Data/EclipseWorkspace/LearningHadoop/SparkExamples/src/main/resources/sample.json"));

		JSONObject jsonObject = (JSONObject) obj;
		JSONObject msg = (JSONObject) jsonObject.get("aa");
		System.out.println(msg);

		JSONArray phno = (JSONArray) msg.get("phoneNumber");
		System.out.println(phno);
		Map<String, String> add = new HashMap<String, String>();
		Iterator iterator = phno.iterator();
		while (iterator.hasNext()) {
			JSONObject number = (JSONObject) iterator.next();
			System.out.println(number.get("number") + " : " + number.get("type"));
			add.put(number.get("type").toString(), number.get("number").toString());
		}

		System.out.println(add.size());

		Iterator iter = add.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Entry) iter.next();
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		
		System.out.println("fax : "+add.get("fax"));
	}

}
