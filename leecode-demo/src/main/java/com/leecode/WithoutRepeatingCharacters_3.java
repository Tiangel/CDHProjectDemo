package com.leecode;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 * 给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度。
 * <p>
 * 示例 1:
 * 输入: "abcabcbb"
 * 输出: 3
 * 解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。
 * 示例 2:
 * 输入: "bbbbb"
 * 输出: 1
 * 解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。
 * 示例 3:
 * 输入: "pwwkew"
 * 输出: 3
 * 解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。
 *      请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。
 */
public class WithoutRepeatingCharacters_3 {
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        String str = "";
        while (scanner.hasNextLine()) {
            str = scanner.nextLine();
            lengthOfLongestSubstring(str);
        }

    }

    /**
     * 暴力法
     * 逐个检查所有的子字符串，看它是否不含有重复的字符。
     */
    public static int lengthOfLongestSubstring(String str) {
        int n = str.length();
        int ans = 0;
        for (int i = 0; i < n; i++){
            for (int j = i + 1; j <= n; j++){
                if (allUnique(str, i, j)) {
                    ans = Math.max(ans, j - i);
                }
            }
        }
        return ans;
    }

    public static boolean allUnique(String s, int start, int end) {
        Set<Character> set = new HashSet<>();
        for (int i = start; i < end; i++) {
            Character ch = s.charAt(i);
            if (set.contains(ch)) {
                return false;
            }
            set.add(ch);
        }
        return true;
    }
}
