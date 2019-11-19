package com.cloudera.vms.logs;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.collect.Tuple;
import org.joda.time.DateTime;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

//import org.apache.logging.log4j.LogManager;


//import org.zhonghong.ams.es.beans.Tuple;

public class LinkLogUtils {
	//private static Logger logger = LoggerFactory.getLogger(LinkLogUtils.class);
	private static org.apache.logging.log4j.Logger logger = LogManager.getLogger(LinkLogUtils.class);
	private static RpcInvokeLog rpclog;
	private static DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static {
		rpclog = new RpcInvokeLog();
	}

	public void getLog(String mid, String proNum) {
		try {
			rpclog.setServerIp(InetAddress.getLocalHost().getHostAddress());
			rpclog.setLogLevel("info");
			rpclog.setLnkKey(mid);
			rpclog.setProcessNum(proNum);
			rpclog.setLogTime(df2.format(Calendar.getInstance().getTime()));
			LinkLogUtils.monitor(rpclog);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	public static void monitor(Object bean) {
		Annotation[] annos = bean.getClass().getAnnotations();
		boolean match = Arrays.stream(annos).anyMatch(a -> a instanceof LogBean);
		if (!match) {
			logger.error("[log util] bean must has annotation {LogBean} !");
			return;
		}
		String log = serilize(bean);
		// System.out.println(log);
		logger.info("[rpc monitor] {}", log);
	}

	public static String serilize(Object bean) {
		LogBean anno = bean.getClass().getAnnotation(LogBean.class);
		Field[] fields = bean.getClass().getDeclaredFields();
		List<Tuple<Integer, Object>> list = Arrays.stream(fields)
				.filter(field -> field.getAnnotation(LogElement.class) != null).map(field -> {
					LogElement a = field.getAnnotation(LogElement.class);
					int rank = a.rank();
					if (rank < 0)
						return null;
					try {
						field.setAccessible(true);
						Object val = field.get(bean);
						return Tuple.tuple(rank, val);
					} catch (Exception e) {
						e.printStackTrace();
					}
					return null;
				}).filter(t -> t != null).collect(Collectors.toList());

		return serilize(list, anno.size(), anno.separator(), anno.placeholder(), anno.pattern());
	}

	private static String serilize(List<Tuple<Integer, Object>> list, int size, char separator, char placeholder,
			String pattern) {
		list.sort((t1, t2) -> t1.v1() - t2.v1());
		List<CharSequence> strs = new ArrayList<>();

		list.forEach(t -> {
			while (strs.size() < t.v1()) {
				strs.add(placeholder + "");
			}
			strs.add(t.v2() == null ? placeholder + "" : t.v2().toString());

		});

		return strs.stream().collect(Collectors.joining(separator + ""));
	}

	public static void main(String[] args) {
		RpcInvokeLog log = new RpcInvokeLog();
		log.setLogLevel("info");
		log.setBeginTime(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
		log.setLogTime(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
		LinkLogUtils.monitor(log);
		logger.error("textss");
		System.out.println(111);
	}
}
