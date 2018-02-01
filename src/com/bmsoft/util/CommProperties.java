package com.bmsoft.util;

import java.io.Serializable;

public class CommProperties implements Serializable{
	private static final long serialVersionUID = -4397407520171693957L;
	
	/**
	 * 报文编码字符集
	 */
	public final static String MSG_BYTE_ENCODE = "utf-8";

	public final static String FORMAT_DATE_DAY = "yyyyMMdd";
    public final static String FORMAT_DATE_MON = "yyyyMM";
    public final static String FORMAT_DATE_HOUR = "yyyyMMddHH";
	public final static String FORMAT_DATE_MIN = "yyyyMMddHHmm";
	public final static String FORMAT_DATE_SEC = "yyyyMMddHHmmss";
	public final static String FORMAT_TIMESTAMP = "yyyy-MM-dd HH24:mm:ss.S";
	public final static String FORMAT_MIN_SEC = "mmss";
    public final static String FORMAT_DATE_DAY2 = "yyyy-MM-dd";

}
