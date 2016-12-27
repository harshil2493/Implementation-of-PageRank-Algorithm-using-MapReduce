package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMapperPhase1 extends
		Mapper<LongWritable, Text, Text, Text> {
	// public static int printData= 1;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// if(printData %10 == 0)
		// {
		String valueRead = value.toString();
		boolean isRedirected = valueRead.contains("<redirect title");
		if (!isRedirected) {
			int startIndexOfTitle = valueRead.indexOf("<title>");
			int endIndexOfTitle = valueRead.indexOf("</title>");

			String titleOfPage = valueRead.substring(startIndexOfTitle + 7,
					endIndexOfTitle);
			if (titleOfPage.contains("/") && titleOfPage.contains("Wikipedia:")) {
				titleOfPage = titleOfPage
						.substring(0, titleOfPage.indexOf("/"));
			}
			if (titleOfPage.contains("#") && titleOfPage.contains("Wikipedia:")) {
				titleOfPage = titleOfPage
						.substring(0, titleOfPage.indexOf("#"));
			}
			if(!titleOfPage.isEmpty())
			{
				context.write(new Text(titleOfPage), new Text(Constants.phase1TitleConstant));
			}
			String startingPage = valueRead;
			boolean stillContainsLinks = valueRead.contains("[[");

			while (stillContainsLinks) {
				try {
					int startIndexOfLink = startingPage.indexOf("[[") + 2;
					int endingIndexOfLink = startingPage.indexOf("]]");
					String connectedLink = startingPage.substring(
							startIndexOfLink, endingIndexOfLink);

					boolean thereIsAnotherLink = connectedLink.contains("[[");

					if (thereIsAnotherLink) {
						// Ignoring Nested
//						String anotherLink = "";
//						if (connectedLink.contains("|")) {
//							anotherLink = connectedLink.substring(0,
//									connectedLink.indexOf("|"));
//
//						}
//						if (connectedLink.contains("]")) {
//							anotherLink = connectedLink.substring(0,
//									connectedLink.indexOf("]"));
//
//						}
//						if (anotherLink.contains("#") && anotherLink.contains("Wikipedia:")) {
//							anotherLink = anotherLink.substring(0,
//									anotherLink.indexOf("#"));
//						}
//						if (anotherLink.contains("|")) {
//							anotherLink = anotherLink.substring(0,
//									anotherLink.indexOf("|"));
//						}
//						if (anotherLink.contains("/")
//								&& anotherLink.contains("Wikipedia:")) {
//							anotherLink = anotherLink.substring(0,
//									anotherLink.indexOf("/"));
//						}
//						if (!anotherLink.isEmpty()) {
//							context.write(new Text(anotherLink), new Text(
//									titleOfPage));
//						}
//						connectedLink = connectedLink.substring(connectedLink
//								.indexOf("[[") + 2);
					}
					else
					{
					if (connectedLink.contains("|")) {
						connectedLink = connectedLink.substring(0,
								connectedLink.indexOf("|"));

					}

					if (connectedLink.contains("#") && connectedLink.contains("Wikipedia")) {
						connectedLink = connectedLink.substring(0,
								connectedLink.indexOf("#"));
					}
					if (connectedLink.contains("/")
							&& connectedLink.contains("Wikipedia:")) {
						connectedLink = connectedLink.substring(0,
								connectedLink.indexOf("/"));
					}
					if (!connectedLink.isEmpty()) {
						context.write(new Text(connectedLink), new Text(
								titleOfPage));
					}

					
					}
					startingPage = startingPage
							.substring(endingIndexOfLink + 2);
					stillContainsLinks = true;
					if (!startingPage.contains("[[")) {
						stillContainsLinks = false;
					}
				} catch (StringIndexOutOfBoundsException stringIndexOutOfBoundsException) {
					System.err.println("Some Invalid String Detacted..");
					stillContainsLinks = false;
				}

			}

			// printData++;
		} else {
			try {
				int startIndexOfTitle = valueRead.indexOf("<title>");
				int endIndexOfTitle = valueRead.indexOf("</title>");

				String titleOfPage = valueRead.substring(startIndexOfTitle + 7,
						endIndexOfTitle);

				// String titleOfPage = valueRead.substring(startIndexOfTitle +
				// 7, endIndexOfTitle);
				if (titleOfPage.contains("/")
						&& titleOfPage.contains("Wikipedia:")) {
					titleOfPage = titleOfPage.substring(0,
							titleOfPage.indexOf("/"));
				}
				if (titleOfPage.contains("#")
						&& titleOfPage.contains("Wikipedia:")) {
					titleOfPage = titleOfPage.substring(0,
							titleOfPage.indexOf("#"));
				}
				int redirectStartingIndex = valueRead.indexOf("<redirect title");
				int redirectEndingIndex = valueRead.indexOf("/>") - 2;
				String redirectTitleTemporary = valueRead.substring(
						redirectStartingIndex, redirectEndingIndex);
				int startOfPage = "<redirect title=\"".length();
				int endOfPage = redirectTitleTemporary.lastIndexOf("\"");
				String redirectTitle = redirectTitleTemporary.substring(startOfPage, endOfPage);
				if(!titleOfPage.isEmpty() && !redirectTitle.isEmpty())
				{
					context.write(new Text(titleOfPage), new Text(Constants.phase1RedirectConstant
						+ redirectTitle));
				}
			} catch (Exception e) {
				System.err.println("Some Invalid String Detacted..");
			}
		}
		// }
	}


}
