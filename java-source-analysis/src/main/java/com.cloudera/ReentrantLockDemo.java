package com.cloudera;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ReentrantLock 的实现原理
 */

/*
Synchronized和ReentrantLock的相同点：

1.ReentrantLock和synchronized都是独占锁,只允许线程互斥的访问临界区。
  但是实现上两者不同:synchronized加锁解锁的过程是隐式的,用户不用手动操作,优点是操作简单，但显得不够灵活。一般并发场景使用synchronized的就够了；
  ReentrantLock需要手动加锁和解锁,且解锁的操作尽量要放在finally代码块中,保证线程正确释放锁。
  ReentrantLock操作较为复杂，但是因为可以手动控制加锁和解锁过程,在复杂的并发场景中能派上用场。
2.ReentrantLock和synchronized都是可重入的。
  synchronized因为可重入因此可以放在被递归执行的方法上,且不用担心线程最后能否正确释放锁；
  而ReentrantLock在重入时要却确保重复获取锁的次数必须和重复释放锁的次数一样，否则可能导致其他线程无法获得该锁。

Synchronized和ReentrantLock的不同点：

1.ReentrantLock是Java层面的实现，synchronized是JVM层面的实现。
2.ReentrantLock可以实现公平和非公平锁。
3.ReentantLock获取锁时，限时等待，配合重试机制更好的解决死锁
4.ReentrantLock可响应中断
5.使用synchronized结合Object上的wait和notify方法可以实现线程间的等待通知机制。
  ReentrantLock结合Condition接口同样可以实现这个功能。而且相比前者使用起来更清晰也更简单。
 */
public class ReentrantLockDemo {
    /*ReentrantLock锁的实现步骤总结为三点
      1、未竞争到锁的线程将会被CAS为一个链表结构并且被挂起。
      2、竞争到锁的线程执行完后释放锁并且将唤醒链表中的下一个节点。
      3、被唤醒的节点将从被挂起的地方继续执行逻辑。*/

    private int num;
    private static final Lock LOCK = new ReentrantLock();

    public void add() {
        try {
            LOCK.lock();
            num++;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LOCK.unlock();
        }
    }


}
