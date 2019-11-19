package com.izhonghong.vms.zookeeper;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtils {

	private static Logger logger = LoggerFactory.getLogger(ZKUtils.class);

	public static void writeDataToZK(String zkPath, String data) throws Exception {

		CuratorFramework zkClient = SimpleZKClient.getZKClient();
		if (null != zkClient) {
			Stat stat = zkClient.checkExists().forPath(zkPath);
			if (null == stat) {
				// 节点不存在
				zkClient.create().creatingParentsIfNeeded().forPath(zkPath, data.getBytes("utf-8"));
			} else {
				// 节点已经存在，则将数据更新到节点中
				zkClient.setData().forPath(zkPath, data.getBytes("utf-8"));
			}

			logger.info("save data to" + zkPath + " success.");
		} else {
			logger.warn("get zk client fail.");
		}
	}

	public static String getDataFromZK(String zkPath) throws Exception {

		CuratorFramework zkClient = SimpleZKClient.getZKClient();
		if (null != zkClient) {
			Stat stat = zkClient.checkExists().forPath(zkPath);
			if (null == stat) {
				return null;
			} else {
				byte[] data = zkClient.getData().forPath(zkPath);
				String result = new String(data, Charset.forName("utf-8"));
				return result;
			}

		} else {
			logger.warn("get zk client fail.");
		}

		return null;
	}

	public static void writeOffsetToZookeeper(String group, String topic, Map<Integer, Long> partitionOffsets)
			throws Exception {

		String zkKafkaOffsetPath = SimpleZKClient.getCfg().get(SimpleZKClient.ZK_KAFKA_OFFSET_PATH);
		CuratorFramework zkClient = SimpleZKClient.getZKClient();
		for (Integer partition : partitionOffsets.keySet()) {
			Long offset = partitionOffsets.get(partition);
			String zkPath = zkKafkaOffsetPath + "/" + group + "/offsets/" + topic + "/" + partition;
			if (null != zkClient) {
				Stat stat = zkClient.checkExists().forPath(zkPath);
				if (null == stat) {
					// 节点不存在
					zkClient.create().creatingParentsIfNeeded().forPath(zkPath, offset.toString().getBytes());
				} else {
					// 节点已经存在，则将数据更新到节点中
					zkClient.setData().forPath(zkPath, offset.toString().getBytes());
				}
				logger.info("offset updated:" + zkPath + ",offset:" + offset);
				logger.info("register offset node" + zkPath + " success.");
			} else {
				logger.warn("get zk client fail.");
			}

		}
	}

	public static Map<Integer, Long> getPartitionOffset(String group, String topic) throws Exception {
		String zkKafkaOffsetPath = SimpleZKClient.getCfg().get(SimpleZKClient.ZK_KAFKA_OFFSET_PATH);
		CuratorFramework zkClient = SimpleZKClient.getZKClient();
		String zkPath = zkKafkaOffsetPath + "/" + group + "/offsets/" + topic;
		Stat stat = zkClient.checkExists().forPath(zkPath);
		if (null == stat) {
			return null;
		}
		Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();
		List<String> childPaths = zkClient.getChildren().forPath(zkPath);
		for (String childPath : childPaths) {
			Integer partition = Integer.parseInt(childPath);
			childPath = zkPath + "/" + childPath;
			byte[] data = zkClient.getData().forPath(childPath);
			String pluginNodeStr = new String(data, Charset.forName("utf-8"));
			// ByteArrayInputStream in = new ByteArrayInputStream(data);
			//
			// DataInputStream oi = new DataInputStream(in);

			Long offset = Long.parseLong(pluginNodeStr);
			partitionOffsets.put(partition, offset);

		}

		return partitionOffsets;
	}

	public static void main(String[] args) throws Exception {
		// String zkPath = "/test";
		// long t1 = Calendar.getInstance().getTimeInMillis();
		// ZKUtils.writeDataToZK(zkPath, "a");
		// long t2 = Calendar.getInstance().getTimeInMillis();
		// String result = ZKUtils.getDataFromZK(zkPath);
		// long t3 = Calendar.getInstance().getTimeInMillis();
		// System.out.println(t2 - t1);
		// System.out.println(t3 - t2);
		Map<Integer, Long> partitionOffset = ZKUtils.getPartitionOffset("zrticle", "exclude_weibo_data");
		System.out.println(partitionOffset);
	}

}
