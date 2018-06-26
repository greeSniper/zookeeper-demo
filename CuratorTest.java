package com.tangzhe.zookeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * zk客户端
 * 测试监听zk节点变化
 */
public class CuratorTest {

	public static void main(String[] args) throws Exception {
		// 1 重试策略：初试时间为1s 重试10次
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
		// 2 通过工厂创建连接
		CuratorFramework cf = CuratorFrameworkFactory.builder()
				.connectString("localhost:2181")
				.sessionTimeoutMs(1000 * 10)
				.retryPolicy(retryPolicy)
				.build();
		// 3 开启连接
		cf.start();
		
//		cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/yjh/aa","aa内容".getBytes());
//		System.out.println(new String(cf.getData().forPath("/yjh/aa")));
//		cf.setData().forPath("/yjh/aa", "修改aa内容".getBytes());
//		System.out.println(new String(cf.getData().forPath("/yjh/aa")));
//		cf.delete().guaranteed().deletingChildrenIfNeeded().forPath("/yjh");
		
//		if (cf.checkExists().forPath("/tt") == null) {
//			cf.create().creatingParentsIfNeeded().forPath("/tt","tt".getBytes());
//		}
//		byte[] data = cf.getData().usingWatcher(new Watcher() {  
//            public void process(WatchedEvent event) {
//                System.out.println("节点监听器 : " + event.getType().getIntValue() + "\t" + event.getPath());  
//            }  
//        }).forPath("/tt");
//		System.out.println(new String(data));
		
		 ExecutorService pool = Executors.newFixedThreadPool(2);
	        
         final NodeCache nodeCache = new NodeCache(cf, "/test", false);
         nodeCache.start(true);
         nodeCache.getListenable().addListener(
            new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    System.out.println(nodeCache.getCurrentData().getPath() + "数据改变了, 新的数据是: " +
                        new String(nodeCache.getCurrentData().getData()));
                }
            }, 
            pool
	        );
		Thread.sleep(Integer.MAX_VALUE);
	}

}
