package search;


public class BinarySearch {
    public static void main(String[] args) {
        int[] arr = new int[]{1,2, 2, 4};
        int data = 2;
        System.out.println(binary(arr, data));

//        int[][] grid = {{1, 1, 1, 1, 0}, {1, 1, 0, 1, 0}, {1, 1, 0, 0, 0}, {0, 0, 0, 0, 0}};
//
//        System.out.println(numIslands(grid));

    }

    public static int binary(int[] arr, int data) {
        int start = 0;
        int end = arr.length - 1;
        // 循环条件为 start < end 时, 可能会导致死循环
        // mid  计算后都会等于 start 的初始值，然后将 mid 赋值给 start，导致 start 和 end 一直紧挨在一起，这时就会导致死循环
        while (start + 1 < end) {
            // 不直接写成 (end + start) / 2 的目的是防止计算越界。
            // start 和 end 很大的时候，会出现溢出的情况，从而导致数组访问出错
            // 假如 end = 2^31 - 1, start = 100,就会在计算 end + start 的时候越界，导致结果不正确。
            int mid = start + ((end - start) >>> 1);
            if (arr[mid] < data) {
                start = mid;
            } else if (arr[mid] > data) {
                end = mid;
            } else {
                return mid;
            }
        }
        return -1;
    }



    //遍历数组，遇到1，则将其周围所有1都置0，循环操作
    public static int numIslands(int[][] grid) {
        if (grid.length == 0 || grid[0].length == 0) {
            return 0;
        }
        int rows = grid.length;
        int cols = grid[0].length;
        int res = 0;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                if (grid[i][j] == 1) {
                    res++;
                    toZero(grid, i, j, rows, cols);
                }
            }
        }
        return res;
    }

    private static void toZero(int[][] grid, int i, int j, int rows, int cols) {
        System.out.println(i + "=====================" + j);
        if (i < 0 || i >= rows || j < 0 || j >= cols) {
            return;
        }
        if (grid[i][j] != 1) {
            return;
        }
        grid[i][j] = 0;
        toZero(grid, i + 1, j, rows, cols);
        toZero(grid, i - 1, j, rows, cols);
        toZero(grid, i, j + 1, rows, cols);
        toZero(grid, i, j - 1, rows, cols);
    }
}
