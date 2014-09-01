package net.iponweb.hadoop.streaming.tools;

import java.util.AbstractMap;
import java.util.Map;

public class KeyValueSplitter {
	private String delimiter;
    private int numKeys = 1;

	public KeyValueSplitter(String delimiter) {
		this.delimiter = delimiter;
	}

    public KeyValueSplitter(String delimiter, int numKeys) {
        this.delimiter = delimiter;
        this.numKeys = numKeys;
    }

	public Map.Entry<String, String> split(String s) {
		int idx = s.indexOf(delimiter);
		if (idx < 0) {
			return new AbstractMap.SimpleEntry<String, String>(s, "");
		}

        int k = 1;
        while (k < numKeys) {
            idx = s.indexOf(delimiter,idx + 1);
            if (idx < 0) break;
            k ++;
        }

        return new AbstractMap.SimpleEntry<String, String>(
                    s.substring(0, idx),
                    s.substring(idx+1)
        );
	}




}
