package org.iot.dsa.servicebus.node;

import org.iot.dsa.io.json.JsonReader;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;

public class Utils {
	
	public static DSMap getMap(DSMap parameters, String key) {
		DSElement elem = parameters.get(key);
		if (elem.isMap()) {
			return elem.toMap();
		} else {
			JsonReader jr = new JsonReader(elem.toString());
			try {
				return jr.getMap();
			} finally {
				jr.close();
			}
		}
	}

}
