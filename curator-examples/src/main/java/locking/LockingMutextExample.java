package locking;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

public class LockingMutextExample {

    //会话超时时间
    private final int SESSION_TIMEOUT = 30 * 1000;

    //连接超时时间
    private final int CONNECTION_TIMEOUT = 10 * 1000;

    //ZooKeeper服务地址
    private static final String SERVER = "zk1.daily.idcvdian.com,zk2.daily.idcvdian.com";

    //创建连接实例
    private CuratorFramework client = null;

    /**
     * baseSleepTimeMs：初始的重试等待时间
     * maxRetries：最多重试次数
     */
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    @BeforeMethod
    public void init() {
        //创建 CuratorFrameworkImpl实例
        //client = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);

        //启动
        //client.start();
    }

    /**
     * 测试创建节点
     *
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {
        //创建永久节点
        //client.create().forPath("/curator","/curator data".getBytes());

        //创建永久有序节点
        //client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/curator_sequential","/curator_sequential data".getBytes());

        //创建临时节点
        //client.create().withMode(CreateMode.EPHEMERAL)
        //.forPath("/toc_trigger","/toc_trigger".getBytes());

        //创建临时有序节点
        //        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        //                .forPath("/curator/ephemeral_path1","/curator/ephemeral_path1 data".getBytes());
        //
        //        client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        //                .forPath("/curator/ephemeral_path2","/curator/ephemeral_path2 data".getBytes());

        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, LOCK_NAME);
        lock.acquire();
        lock.release();
    }

    /**
     * 检查节点是否存在
     * @throws Exception
     */
    @Test
    public void testCheck() throws Exception {
        Stat stat1 = client.checkExists().forPath("/toc_trigger");
        Stat stat2 = client.checkExists().forPath("/curator2");

        System.out.println("/toc_trigger是否存在： " + (stat1 != null ? true : false));
        System.out.println("'/curator2'是否存在： " + (stat2 != null ? true : false));
    }

    /**
     * 下面两个方法模拟如果有一个机器挂掉，另一个机器会从相同的path路径拿到锁
     * @throws Exception
     */
    @Test
    public void testCreate2() throws Exception {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client1.start();

        InterProcessSemaphoreMutex lock1 = new InterProcessSemaphoreMutex(client1, LOCK_NAME);
        lock1.acquire();
        lock1.release();
        System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
    }

    @Test
    public void testCreate4() throws Exception {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client1.start();

        for (int i = 0; i < 3; i++) {
            InterProcessSemaphoreMutex lock1 = new InterProcessSemaphoreMutex(client1, LOCK_NAME);
            try {
                lock1.acquire(2, TimeUnit.HOURS);
                System.out.println("11");
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
            }
            System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
        }

        LockSupport.park();

    }

    @Test
    // 测试结果：
    // 只会往根节点创建一个临时节点，并获取锁
    // 接下来的for循环不会再创建节点，而是锁计数器累加
    public void 单个对象可重入锁() throws Exception {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client1.start();
        InterProcessMutex lock1 = new InterProcessMutex(client1, LOCK_NAME);
        try {
            for (int i = 0; i < 3; i++) {
                lock1.acquire();
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
        System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
    }

    @Test
    // 测试结果：
    // 如果是并发：则会创建三个节点，第一个抢锁后，后面的排序进行等待
    // 如果是同步：则会创建2个节点，第一个抢锁后，第二个任务会阻塞，第三个任务被第二个任务阻塞运行了
    public void 多个对象可重入锁() throws Exception {
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client1.start();
        try {
            for (int i = 0; i < 3; i++) {
                InterProcessMutex lock1 = new InterProcessMutex(client1, LOCK_NAME);
                lock1.acquire();
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
        System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
    }

    @Test
    // 测试结果：
    //InterProcessSemaphoreMutex 低层实现是InterProcessMutex
    //  InterProcessMutex单独使用时候，则会在当前目录下创建lock节点,但在InterProcessSemaphoreMutex这里，会创建locks节点下，在去创建临时有序节点
    //  如果leases抢锁成功，则会在节点下创建，临时节点，并之前在locks下创建的临时节点
    public void 单个互斥锁() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client.start();

        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, LOCK_NAME);
        lock.acquire();
        System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
    }


    @Test
    // 测试结果：
    // 如果是并发：则会创建三个节点，第一个抢锁后，后面的排序进行等待
    // 如果是同步：则会创建2个节点，第一个抢锁后，第二个任务会阻塞，第三个任务被第二个任务阻塞运行了
    public void 多个对象互斥锁() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client.start();
        try {
            for (int i = 0; i < 3; i++) {
                InterProcessSemaphoreMutex lock1 = new InterProcessSemaphoreMutex(client, LOCK_NAME);
                lock1.acquire();
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
        System.out.println("Thread: " + Thread.currentThread().getName() + "has the lock");
    }

    private static final int QTY = 3;
    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("demo-poll-%d").build();
    private static ExecutorService threadPool = new ThreadPoolExecutor(5, 200, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1024), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    private static final String LOCK_NAME = "/toc/trigger";

    @Test
    public void testCreate3() throws Exception {
        for (int i = 0; i < QTY; ++i) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    CuratorFramework clientTest = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
                    clientTest.start();
                    InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(clientTest, LOCK_NAME);
                    try {
                        while (true) {
                            Thread.sleep(2000);
                            boolean acquiredInThisProcess = lock.acquire(0, TimeUnit.SECONDS);
                            if (acquiredInThisProcess) {
                                System.out.println("Thread: " + Thread.currentThread().getName() + " has the lock");
                                System.out.println("Thread: " + Thread.currentThread().getName() + " is working...");
                                Thread.sleep(2000);
                                System.out.println("Thread: " + Thread.currentThread().getName() + " work complete");
                                //lock.release();
                                //System.out.println("Thread: " + Thread.currentThread().getName() + " release the lock");
                                System.out.println("--------------------------------------------------------");
                            } else {
                                System.out.println("Thread: " + Thread.currentThread().getName() + "could not acquired the lock");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Thread：" + Thread.currentThread().getName() + " lock release failed");
                        e.printStackTrace();
                    }
                }
            };
            threadPool.submit(runnable);
        }
        threadPool.shutdown();
        LockSupport.park();
    }

    private static final String DELETE_LOCK_NAME_LOCKS   = "/toc/trigger/locks";
    private static final String DELETE_LOCK_NAME_RELEASE = "/toc/trigger/leases";
    private static final String DELETE_LOCK_NAME         = "/toc/trigger";

    @Test
    public void testDelete() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECTION_TIMEOUT, retryPolicy);
        client.start();
        client.delete().forPath(DELETE_LOCK_NAME_LOCKS);
        client.delete().forPath(DELETE_LOCK_NAME_RELEASE);
        client.delete().forPath(DELETE_LOCK_NAME);
    }
}
