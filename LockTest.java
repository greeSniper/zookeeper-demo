package com.tangzhe.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * zk分布式锁
 */
public class LockTest {
	
	static int count = 2;

	public static void main(String[] args) {
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
		final InterProcessMutex lock = new InterProcessMutex(cf, "/mylock");
		final CountDownLatch latch = new CountDownLatch(1);
		ExecutorService pool = Executors.newFixedThreadPool(20);
		for (int i = 0; i < 20; i++) {
			pool.execute(() -> {
				try {
					System.err.println(1);
					latch.await();
					lock.acquire();
					Thread.sleep(100);
					//synchronized (LockTest.class) {
						if (count > 0) {
							count--;
							lock.release();
							System.out.println(count);
						}
						
					//}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
		System.out.println("开始执行");
		latch.countDown();
		pool.shutdown();
	}

}
