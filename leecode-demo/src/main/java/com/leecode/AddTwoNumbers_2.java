package com.leecode;

/**
 * 给出两个非空的链表用来表示两个非负的整数。其中，它们各自的位数是按照逆序的方式存储的，
 * 并且它们的每个节点只能存储一位数字。
 * 如果，我们将这两个数相加起来，则会返回一个新的链表来表示它们的和。
 * 您可以假设除了数字 0 之外，这两个数都不会以 0 开头。
 * <p>
 * 示例：
 * 输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
 * 输出：7 -> 0 -> 8
 * 原因：342 + 465 = 807
 */
public class AddTwoNumbers_2 {
    public static void main(String[] args) {
        ListNode node1 = new ListNode(2);
        node1.setNext(new ListNode(4));
        node1.getNext().setNext(new ListNode(3));

        ListNode node2 = new ListNode(5);
        node2.setNext(new ListNode(6));
        node2.getNext().setNext(new ListNode(4));
        ListNode resultNode = addTwoNumbers(node1, node2);
        StringBuilder sb = new StringBuilder();
        while (resultNode != null){
            sb.append(resultNode.getVal());
            resultNode = resultNode.getNext();
        }
        System.out.println(sb.reverse());
    }

    public static ListNode addTwoNumbers(ListNode node1, ListNode node2) {
        ListNode dummyHead = new ListNode(0);
        ListNode curr = dummyHead;
        int carry = 0;
        while (node1 != null || node2 != null) {
            int x = (node1 != null) ? node1.getVal() : 0;
            int y = (node2 != null) ? node2.getVal() : 0;
            int sum = carry + x + y;
            carry = sum / 10;
            curr.setNext(new ListNode(sum % 10));
            curr = curr.getNext();
            if (node1 != null) {
                node1 = node1.getNext();
            }
            if (node2 != null) {
                node2 = node2.getNext();
            }
        }
        if (carry > 0) {
            curr.setNext(new ListNode(carry));
        }
        return dummyHead.getNext();
    }
}

class ListNode {
    private int val;
    private ListNode next;

    public ListNode(int x) {
        val = x;
    }

    public int getVal() {
        return val;
    }

    public void setVal(int val) {
        this.val = val;
    }

    public ListNode getNext() {
        return next;
    }

    public void setNext(ListNode next) {
        this.next = next;
    }
}
