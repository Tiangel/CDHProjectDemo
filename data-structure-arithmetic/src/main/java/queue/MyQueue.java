package queue;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Charles
 * @package queue
 * @classname MyQueue
 * @description TODO
 * @date 2019-9-19 20:23
 */
// "static void main" must be defined in a public class.
public class MyQueue {
    // store elements
    private List<Integer> data;
    // a pointer to indicate the start position
    private int p_start;
    public MyQueue() {
        data = new ArrayList<Integer>();
        p_start = 0;
    }
    /** Insert an element into the queue. Return true if the operation is successful. */
    public boolean enQueue(int x) {
        data.add(x);
        return true;
    };
    /** Delete an element from the queue. Return true if the operation is successful. */
    public boolean deQueue() {
        if (isEmpty() == true) {
            return false;
        }
        p_start++;
        return true;
    }
    /** Get the front item from the queue. */
    public int Front() {
        return data.get(p_start);
    }
    /** Checks whether the queue is empty or not. */
    public boolean isEmpty() {
        return p_start >= data.size();
    }

//    public static void main(String[] args) {
//        MyQueue q = new MyQueue();
//        q.enQueue(5);
//        q.enQueue(3);
//        if (q.isEmpty() == false) {
//            System.out.println(q.Front());
//        }
//        q.deQueue();
//        if (q.isEmpty() == false) {
//            System.out.println(q.Front());
//        }
//        q.deQueue();
//        if (q.isEmpty() == false) {
//            System.out.println(q.Front());
//        }
//    }


    public static void main(String[] args) {

        // 利用或操作 | 和空格将英文字符转换为小写
        System.out.println((char)('a' | ' ')); // a
        System.out.println((char)('A' | ' ')); // a
        // 利用与操作 & 和下划线将英文字符转换为大写
        System.out.println((char)('b' & '_')); // B
        System.out.println((char)('B' & '_')); // B
        // 利用异或操作 ^ 和空格进行英文字符大小写互换
        System.out.println((char)('d' ^ ' ')); // D
        System.out.println((char)('D' ^ ' ')); // d

        // 判断两个数是否异号
        // 使用乘积或者商来判断两个数是否异号，但是这种处理方式可能造成溢出
        System.out.println((-1 ^ 2) < 0); // true
        System.out.println((3 ^ 2) < 0); // false


        int a = 1, b = 2;
        a ^= b;
        System.out.println(a + "=======" +  b);
        b ^= a;
        System.out.println(a + "=======" +  b);
        a ^= b;
        System.out.println(a + "=======" +  b);

        System.out.println(6 & 2 );


    }
}