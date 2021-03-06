package org.iot.dsa.servicebus;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMetadata;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;


/**
 * Miscellaneous utility methods.
 *
 * @author Daniel Shapiro
 */
public class Util {

    public static void putInMap(DSMap map, String key, Object value) {
        if (value instanceof Long) {
            map.put(key, (Long) value);
        } else if (value instanceof Integer) {
            map.put(key, (Integer) value);
        } else if (value instanceof Number) {
            map.put(key, ((Number) value).doubleValue());
        } else if (value instanceof Boolean) {
            map.put(key, (Boolean) value);
        } else {
            map.put(key, value.toString());
        }
    }

    public static DSMap makeColumn(String name, DSValueType type) {
        return new DSMetadata().setName(name).setType(type).getMap();
    }

    public static DSMap makeParameter(String name, DSElement def, MyValueType type,
            String description, String placeholder) {
        DSMap param = new DSMap();
        if (name == null || (def == null && type == null)) {
            return param;
        }
        param.put("name", name);
        if (def != null) {
            param.put("default", def);
        }
        if (type != null) {
            param.put("type", type.toString());
        }
        if (description != null) {
            param.put("description", description);
        }
        if (placeholder != null) {
            param.put("placeholder", placeholder);
        }
        return param;
    }


    public static class MyValueType {

        private DSValueType type;
        private List<String> states;

        private MyValueType(DSValueType type) {
            this(type, null);
        }

        private MyValueType(DSValueType type, List<String> states) {
            this.type = type;
            this.states = states;
        }

        public static final MyValueType BINARY = new MyValueType(DSValueType.BINARY);
        public static final MyValueType BOOL = new MyValueType(DSValueType.BOOL);
        public static final MyValueType ENUM = new MyValueType(DSValueType.ENUM);
        public static final MyValueType LIST = new MyValueType(DSValueType.LIST);
        public static final MyValueType MAP = new MyValueType(DSValueType.MAP);
        public static final MyValueType NUMBER = new MyValueType(DSValueType.NUMBER);
        public static final MyValueType STRING = new MyValueType(DSValueType.STRING);

        public static MyValueType enumOf(List<String> states) {
            return new MyValueType(DSValueType.ENUM, states);
        }

        public static MyValueType boolOf(String trueString, String falseString) {
            return new MyValueType(DSValueType.BOOL, Arrays.asList(trueString, falseString));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(type.toString());
            if (states != null) {
                sb.append("[").append(StringUtils.join(states, ',')).append("]");
            }
            return sb.toString();
        }

        public DSString encode() {
            return DSString.valueOf(toString());
        }

    }

}
