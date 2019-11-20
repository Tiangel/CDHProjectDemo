package com.cloudera.phoenixdemo.constant;

public class IMChatConstant {
    public static final String APPLICATION_JSON = "application/json";
    public static final String METHOD_GET = "GET";
    public static final String METHOD_POST = "POST";
    public static final String COMMUNITY_IM_CHAT_INDEX = "community_im_chat";
    public static final String COMMUNITY_IM_CHAT_QUERY_OR_CONDITION = "queryOrCondition";
    public static final String COMMUNITY_IM_CHAT_QUERY_AND_CONDITION = "queryAndCondition";

    public static final String COMMUNITY_IM_CHAT_ORDERBY_CLAUSE = "orderByClause";

    public static final String COMMUNITY_IM_CHAT_FROMNUM = "fromNum";
    public static final String COMMUNITY_IM_CHAT_QUERYSIZE = "querySize";
    public static final int COMMUNITY_IM_CHAT_DEFAULT_FROM_NUM = 0;
    public static final int COMMUNITY_IM_CHAT_MAX_PAGE_SIZE = 1000;
    public static final int COMMUNITY_IM_CHAT_DEFAULT_PAGE_SIZE = 20;
    public static final int COMMUNITY_IM_CHAT_TIMEOUT = 60;
    public static final String IM_CHAT_MEDIA_TYPE = "payload.bodies.type";
    public static final String IM_CHAT_MEDIA_TYPE_TXT = "txt";
    public static final String IM_CHAT_MEDIA_TYPE_IMG = "img";
    public static final String IM_CHAT_MEDIA_TYPE_LOC = "loc";
    public static final String IM_CHAT_MEDIA_TYPE_AUDIO = "audio";
    public static final String IM_CHAT_MEDIA_TYPE_VIDEO = "video";
    public static final String IM_CHAT_MEDIA_TYPE_FILE = "file";

    public static final String IM_CHAT_QUERY_FROM=  "from";
    public static final String IM_CHAT_QUERY_TO=  "to";
    public static final String IM_CHAT_QUERY_MSG_ID=  "msg_id";
    public static final String IM_CHAT_QUERY_CHAT_TYPE = "chat_type";
    public static final String IM_CHAT_QUERY_MSG = "payload.bodies.msg";
    public static final String IM_CHAT_QUERY_TIMESTAMP = "timestamp";
    public static final String IM_CHAT_QUERY_TIMESTAMP_BEGINDATE = "beginDate";
    public static final String IM_CHAT_QUERY_TIMESTAMP_ENDDATE = "endDate";

    /**角色类型 ALL-全部  RESIDENT-业主  HKEEPER-管家**/
    public static final String IM_CHAT_QUERY_ROLETYPE_ALL = "ALL";
    public static final String IM_CHAT_QUERY_ROLETYPE_RESIDENT = "RESIDENT";
    public static final String IM_CHAT_QUERY_ROLETYPE_HKEEPER = "HKEEPER";
}

