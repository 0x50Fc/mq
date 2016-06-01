package org.hailong.mq;

public class Value {

	public static final long longValue(Object v,long defaultValue) {
		if(v != null) {
			if(v instanceof Number) {
				return ((Number) v).longValue();
			}
			else if(v instanceof String) {
				return Long.valueOf((String)v);
			}
		}
		return defaultValue;
	}
	
	public static final String stringValue(Object v,String defaultValue) {
		if(v != null) {
			if(v instanceof String) {
				return ((String) v);
			}
			else {
				return String.valueOf(v);
			}
		}
		return defaultValue;
	}
	
}
