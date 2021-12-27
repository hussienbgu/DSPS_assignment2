package com.amazonaws.dsps2021;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Step0 {

	public static class Mapper0 extends Mapper<Object, Text, Text, Text> {

		private String w1;
		private String w2;
		private String count;
		private int year;
		private int decade;

		public final static HashSet<String> hebrewStopWords =
				new HashSet<>(Arrays.asList("של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה"
						, "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
						"יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
						"היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
						"אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"","״","׳",
						"!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע",
						"זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי",
						"אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
						"לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין",
						"בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"));

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] ngramEntry = itr.nextToken().split("\t");
				try {
					w1 = ngramEntry[0].split(" ")[0];
					w2 = ngramEntry[0].split(" ")[1];
					year = Integer.parseInt(ngramEntry[1]);
					decade = (year / 10) * 10;
					count = ngramEntry[2];
					if (hebrewStopWords.contains(w1) || hebrewStopWords.contains(w2))
						continue;
				} catch (IndexOutOfBoundsException e) {
					continue;
				}

				Text newEntry = new Text(Integer.toString(decade) + "\t" + w1 + "\t" + w2);
				context.write(newEntry, new Text(count));
				// <key,value> : <decade w1 w2 , count>
				// <1990 hi hello , 500>
				// <1990 hi hello , 400>
			}
		}
	}

	public static class Reducer0 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// <key,value> = < decade  w1  w2 , count>
			int sum = 0;
			for (Text val : values) {
				sum += Integer.valueOf(val.toString());
			}
			context.write(key, new Text(String.valueOf(sum)));
			// <decade  w1  w2 , c(w1w2)>
		}
	}


}
