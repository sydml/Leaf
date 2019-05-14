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
    /**
     * 线程池，用于执行异步任务，比如异步准备双buffer中的另一个buffer
     */
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    /**
     * 标记自己是否初始化完毕
     */
    private volatile boolean initOK = false;
    /**
     * cache，存储所有业务key对应双buffer号段，所以是基于内存的发号方式
     */
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    /**
     * 查询数据库的dao
     */
    private IDAllocDao dao;
    /**
     * 执行异步准备Segment任务的线程池的ThreadFactory
     */
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

    /**
     * 每分钟同步db到cache
     */
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

    /**
     * 将数据库表中的tags同步到cache中
     */
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            // 获取数据库表中所有的biz_tag
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            // 获取当前的cache中所有的tag
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            // 数据库中的tag
            List<String> insertTags = new ArrayList<String>(dbTags);
            List<String> removeTags = new ArrayList<String>(cacheTags);

            // 下面两步操作：保证cache和数据库tags同步
            // 1. cache新增上数据库表后添加的tags
            // 2. cache删除掉数据库表后删除的tags

            // 1. db中新加的tags灌进cache
            insertTags.removeAll(cacheTags);
            for (String tag : insertTags) {
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            // 2. cache中已失效的tags从cache删除
            removeTags.removeAll(dbTags);
            for (String tag : removeTags) {
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
     * 获取对应key的下一个id
     * @param key
     * @return
     */
    @Override
    public Result get(final String key) {
        // 必须在 SegmentIDGenImpl 初始化后执行init()方法
        if (!initOK) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            // 获取cache中对应的SegmentBuffer
            SegmentBuffer buffer = cache.get(key);

            // 双重判断，避免重复执行SegmentBuffer的初始化操作.
            if (!buffer.isInitOk()) {
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        // 初始化SegmentBuffer
                        try {
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            return getIdFromSegmentBuffer(cache.get(key));
        }

        // cache中不存在对应的key
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 从数据库中更新SegmentBuffer中的Segment
     * @param key
     * @param segment
     */
    public void updateSegmentFromDb(String key, Segment segment) {
        StopWatch sw = new Slf4JStopWatch();

        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        // 如果buffer没有初始化(第一次初始化)
        if (!buffer.isInitOk()) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            buffer.setStep(leafAlloc.getStep());
            // leafAlloc中的step为DB中的step，buffer未初始化，所以最小是leafAlloc中的step
            buffer.setMinStep(leafAlloc.getStep());
        }
        // 如果buffer的更新时间是0（初始是0，也就是第二次进来）
        else if (buffer.getUpdateTimestamp() == 0) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            // 第二次更新更新时间
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            // leafAlloc中的step为DB中的step
            buffer.setMinStep(leafAlloc.getStep());
        }
        // 第三次以及之后的进来 动态设置nextStep
        else {
            // 计算当前更新操作和上一次更新时间差
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();

            /**
             *  动态调整step
             *  1) duration < 15 分钟 : step 变为原来的2倍， 最大为 MAX_STEP
             *  2) 15分钟 <= duration < 30分钟 : nothing
             *  3) duration >= 30 分钟 : 缩小step, 最小为DB中配置的步数
             *
             *  这样做的原因是认为15min一个号段大致满足需求
             *  如果updateSegmentFromDb()速度频繁(15min多次)，也就是
             *  如果15min这个时间就把step号段用完，为了降低数据库访问频率，我们可以扩大step大小
             *  相反如果将近30min才把号段内的id用完，则可以缩小step
             */

            // duration < 15 分钟 : step 变为原来的2倍. 最大为 MAX_STEP
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    // 步数 * 2
                    nextStep = nextStep * 2;
                }
            }
            // 15分钟 < duration < 30分钟 : nothing
            else if (duration < SEGMENT_DURATION * 2) {
                //do nothing with nextStep
            }
            // duration > 30 分钟 : 缩小step ,最小为DB中配置的步数
            else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);

            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            // 更新数据库maxId
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(nextStep);
            // leafAlloc的step为DB中的step
            buffer.setMinStep(leafAlloc.getStep());
        }
        // must set value before set max
        long value = leafAlloc.getMaxId() - buffer.getStep();
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    /**
     * 从SegmentBuffer获取id
     * @param buffer
     * @return
     */
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        // 自旋获取
        while (true) {
            try {
                // 获取buffer的共享读锁，在平时不操作Segment的情况下益于并发
                buffer.rLock().lock();

                // 获取当前正在使用的Segment
                final Segment segment = buffer.getCurrent();

                // ===============异步准备双buffer的另一个Segment==============
                // 1. 另一个Segment没有准备好
                // 2. 当前Segment已经使用超过10%则开始异步准备另一个Segment
                // 3. buffer中的threadRunning字段. 代表是否已经提交线程池运行，是否有其他线程已经开始进行另外号段的初始化工作.使用CAS进行更新保证buffer在任意时刻,只会有一个线程进行异步更新另外一个号段.
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    // 线程池异步执行【准备Segment】任务
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            // 获得另一个Segment对象
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                // 从数据库表中准备Segment
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                // 如果准备成功，则通过独占写锁设置另一个Segment准备标记OK，threadRunning为false表示准备完毕
                                if (updateOk) {
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    // 失败了，则还是没有准备好，threadRunning恢复false，以便于下次获取id时重新再异步准备Segment
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }

                // 原子value++，也就是下一个id，这一步是多线程操作的，每一个线程加1都是原子的，但不一定保证顺序性
                long value = segment.getValue().getAndIncrement();
                // 如果获取到的id小于maxId
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                // 释放读锁
                buffer.rLock().unlock();
            }

            // 等待线程池异步准备号段完毕
            waitAndSleep(buffer);

            // 执行到这里，说明当前号段已经用完，应该切换另一个Segment号段使用
            try {
                // 获取独占式写锁
                buffer.wLock().lock();
                // 获取当前使用的Segment号段
                final Segment segment = buffer.getCurrent();
                // 重复获取value, 多线程执行时,在进行waitAndSleep()后,current segment可能会被修改.在交换Segment前进再次判断, 防止出错
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }

                // 执行到这里, 说明其他的线程没有进行Segment切换，并且当前号段所有号码用完，需要进行切换Segment
                // 如果准备好另一个Segment，直接切换
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                }
                // 如果另一个Segment没有准备好，则返回异常双buffer全部用完
                else {
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                // 释放写锁
                buffer.wLock().unlock();
            }
        }
    }

    /**
     * 自旋超时睡眠，如果自旋10000以内，线程池执行【准备Segment任务】结束就直接退出，否则就睡眠10ms，防止CPU空转
     * @param buffer
     */
    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    Thread.currentThread().sleep(10);
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
