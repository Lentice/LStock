package lstockv2;

import org.jsoup.nodes.Element;

public class HtmlUtil {

	public static String getPercent(Element el) {
		String text = getText(el);
		if (text.isEmpty())
			return null;

		text = text.replace("%", "");

		float value = Float.parseFloat(text) / 100;
		return String.valueOf(value);
	}

	public static String getText(Element el) {
		String text = trim(el.text().trim().replaceAll(",", ""));
		if (text.isEmpty() || "不適用".equals(text))
			return null;

		return text;
	}
	
	public static Integer getInt(Element el) {
		try {		
			return Integer.parseInt(getText(el));
		} catch (NumberFormatException ex) {
			return null;
		}
	}
	
	public static Long getLong(Element el) {
		try {		
			return Long.parseLong(getText(el));
		} catch (NumberFormatException ex) {
			return null;
		}
	}
	
	public static Float getFloat(Element el) {
		try {		
			return Float.parseFloat(getText(el));
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	public static String trim(String s) {
		if (s.isEmpty())
			return s;

		int begin = s.length();
		int end = -1;

		for (int i = 0; i < s.length(); i++) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				begin = i;
				break;
			}
		}
		s = s.substring(begin, s.length());
		if (s.isEmpty())
			return s;

		for (int i = s.length() - 1; i >= 0; i--) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				end = i;
				break;
			}
		}

		return s.substring(0, end + 1);
	}

}
