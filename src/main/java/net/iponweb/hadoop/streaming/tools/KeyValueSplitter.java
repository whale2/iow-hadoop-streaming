/**
 * Copyright 2014 IPONWEB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
