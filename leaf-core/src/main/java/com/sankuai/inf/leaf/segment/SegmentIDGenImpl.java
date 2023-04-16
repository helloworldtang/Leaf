package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    /**
     * 使用volatile解决变量initOK对多线程的可见性问题
     */
    private volatile boolean initOK = false;
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    private IDAllocDao dao;

    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    @Override
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        updateCacheFromDb();
        initOK = true;
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                t.setDaemon(true);
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);
            //db中新加的tags灌进cache
            for (int i = 0; i < cacheTags.size(); i++) {
                String tmp = cacheTags.get(i);
                if (insertTagsSet.contains(tmp)) {
                    insertTagsSet.remove(tmp);
                }
            }
            for (String tag : insertTagsSet) {
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            //cache中已失效的tags从cache删除
            for (int i = 0; i < dbTags.size(); i++) {
                String tmp = dbTags.get(i);
                if (removeTagsSet.contains(tmp)) {
                    removeTagsSet.remove(tmp);
                }
            }
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    /**
     * 双buffer架构获取分布式ID
     *
     * @param key 业务标识
     * @return 新的分布式ID：业务ID+最大的数字
     */
    @Override
    public Result get(final String key) {
        /**
         * 检查数据库里的业务标识biz_tag、步长step、max_id都加载到内存中了
         */
        if (!initOK) {
            /**
             * 没有加载完成，后面没有check的逻辑，得到的分布式ID很大可能性是不对的。
             * 要确保服务的确定性，此时抛出异常是合适的
             */
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            /**
             * 如果业务标识存在，则要使用步长step和max_id来生成分布式ID
             */
            SegmentBuffer buffer = cache.get(key);
            if (!buffer.isInitOk()) {
                /**
                 * 如果max_id数据没有准备好，则
                 */
                synchronized (buffer) {
                    /**
                     * 不同的Jvm是不同的号段。
                     * 只需要在jvm范围内确保线程安全即可
                     */
                    if (!buffer.isInitOk()) {
                        try {
                            /**
                             * 更新号段
                             */
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            /**
             * 如果数据库里的数据初始化到jvm内存完成，直接getAndIncrement获取自增的id
             */
            return getIdFromSegmentBuffer(cache.get(key));
        }
        /**
         * 如果业务标识key在jvm中不存在，则无法继续后续处理，抛异常
         */
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 更新号段
     * (1)设置号段的起始值：maxId-step
     * (2)设置jvm当前号段的maxId
     * (3)设置步长step
     *
     * @param key     指定的业务标识biz_tag
     * @param segment 存放了新的号段范围的Segment
     */
    public void updateSegmentFromDb(String key, Segment segment) {
        StopWatch sw = new Slf4JStopWatch();
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        if (!buffer.isInitOk()) {
            /**
             * 如果jvm中的号段信息没有完成db到jvm的初始化
             * 则：
             * (1)获取下一个号段的值
             * maxId=maxId+step
             * (2)更新jvm中的step，有可能step被更新过了
             *
             * updateMaxIdAndGetLeafAlloc的说明：
             * (1)更新maxId
             * (2)查询最新maxId,step的操作要在一个事务中。
             * 这个地方展开讲一下：此处是使用数据库更新行锁来达到一个全局互斥锁的效果
             *
             */
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else if (buffer.getUpdateTimestamp() == 0) {
            /**
             * 第一次更新号段信息：
             * (1)获取下一个号段的值 maxId=maxId+step
             * (2)更新jvm中的step，有可能step被更新过了
             * (3)更新updateTimestamp
             *
             * updateMaxIdAndGetLeafAlloc的说明：
             * (1)更新maxId
             * (2)查询最新maxId,step的操作要在一个事务中。
             * 这个地方展开讲一下：此处是使用数据库更新行锁来达到一个全局互斥锁的效果
             *
             */
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else {
            /**
             * 如果号段消费超过预期，且step小于最大步长不超过100,0000
             * 则按下面的规则加大step
             */
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {
                //do nothing with nextStep
            } else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f", ((double) duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            /**
             *
             * updateMaxIdByCustomStepAndGetLeafAlloc说明：
             * (1)更新maxId、step
             * (2)查询最新maxId,step的操作要在一个事务中。
             * 这个地方展开讲一下：此处是使用数据库更新行锁来达到一个全局互斥锁的效果
             * 与updateMaxIdAndGetLeafAlloc相比，多更新了step
             */
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        // must set value before set max
        /**
         * (1)设置号段的起始值：maxId-step
         * (2)设置jvm当前号段的maxId
         * (3)设置步长step
         */
        long value = leafAlloc.getMaxId() - buffer.getStep();
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    /**
     * 从SegmentBuffer中获取biz_tag对应号段在内存中的最大值
     * 最大值超出当前号段的最大值后，要更新jvm中的号段，然后再获取最大值
     *
     * @param buffer
     * @return 当前jvm中biz_tag对应号段在内存中的最大值
     */
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        /**
         * 为解决超出jvm已经申领号段范围的问题,增加while(true)
         * 1. 如果超过jvm中的号段范围，更新下一个号段
         * 2. 将最新的max_id更新到数据库，
         * 3、将最新的数据加载到jvm，
         * 4、然后再获取当前号段的getAndIncrement
         */
        while (true) {
            /**
             * 使用ReadWriteLock解决getAndIncrement获取最新id时的线程安全问题
             */
            buffer.rLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    /**
                     * 如果（1）下一个号段没有ready
                     * 且（2）当前号段已下发10%时，如果下一个号段未更新，
                     * 且（3）更新下一个号段的线程
                     * 则另启一个更新线程去更新下一个号段
                     */
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            /**
                             * 开始准备下一个号段的号段范围
                             */
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            /**
                             * 一个标识，用来标识下一个号段范围是否更新完成
                             */
                            boolean updateOk = false;
                            try {
                                /**
                                 * 执行 更新下一个号段 的操作
                                 */
                                updateSegmentFromDb(buffer.getKey(), next);
                                /**
                                 * 号段范围更新完成，更新标识
                                 */
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                if (updateOk) {
                                    /**
                                     * 更新状态时，打开写锁。获取id的过程都卡住
                                     */
                                    buffer.wLock().lock();
                                    /**
                                     * 下一个号段已更新完成
                                     */
                                    buffer.setNextReady(true);
                                    /**
                                     * 更新号段的线程的状态设置为非运行中
                                     */
                                    buffer.getThreadRunning().set(false);
                                    /**
                                     * 更新下一个号段的所有状态更新完成，释放写锁，当前jvm可以正常生成指定biz_tag的分布式id
                                     */
                                    buffer.wLock().unlock();
                                } else {
                                    /**
                                     * 下一个号段没有更新成功，则将更新线程退出前将线程状态置为false
                                     */
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                /**
                 * getAndIncrement来获取下一个id
                 */
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    /**
                     * 如果新的id在号段范围内，则获取成功
                     */
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                /**
                 * 第一轮获取id，unlock。
                 * 因为readLock与writeLock是互斥，此处不释放readLock则会阻塞 更新线程去更新下一个号段
                 */
                buffer.rLock().unlock();
            }
            /**
             * 自旋等待更新号段线程执行完成
             */
            waitAndSleep(buffer);
            /**
             * 因为当前并不能100%确定更新线程已经成功执行完成
             * 加写锁。本方法入口处的 buffer.rLock().lock();也会被阻塞
             */
            buffer.wLock().lock();
            try {
                final Segment segment = buffer.getCurrent();
                /**
                 * getAndIncrement获取id
                 */
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                /**
                 * 走到此处，说明更新线程还没有执行完成
                 */
                if (buffer.isNextReady()) {
                    /**
                     * 更新线程操作完成。
                     * 则切换SegmentBuffer
                     */
                    buffer.switchPos();
                    /**
                     * 如果新SegmentBuffer用完了，然后再启动更新线程
                     * 更新线程的运行状态置为false。
                     */
                    buffer.setNextReady(false);
                } else {
                    /**
                     * 到此处，说明两个SegmentBuffer都没有ready，是出问题了
                     */
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                /**
                 * 释放wLock，不然这个整个方法就会被阻塞了
                 */
                buffer.wLock().unlock();
            }
        }
    }

    /**
     * 等待号段更新线程更新完成
     * 最多等待100s
     *
     * @param buffer
     */
    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if (roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted", Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
