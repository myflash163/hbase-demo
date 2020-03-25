public class Test {
    public static void main(String[] args) {
        String haystack = "hello";
        String needle = "ll";

        System.out.println(strStr("abcd", "d"));

    }

    public static int strStr(String haystack, String needle) {
        if (haystack == null) {
            return -1;
        }
        if (needle == null) {
            return -1;
        }
        if (needle.length() == 0) {
            return 0;
        }
        char[] hayArray = haystack.toCharArray();
        char[] needleArray = needle.toCharArray();
        int length = hayArray.length - needleArray.length + 1;
        for (int i = 0; i < length; i++) {
            int j = 0;
            for (; j < needleArray.length; j++) {
                if (hayArray[i+j] != needleArray[j]) {
                    break;
                }
            }
            if (j == needleArray.length) {
                return i;
            }
        }
        return -1;
    }

    public static int removeElement(int[] nums, int val) {
        if (nums == null) return 0;
        int length = nums.length;
        if (length == 0) return 0;
        int index = 0;
        for (int i = 0; i < length; i++) {
            if (nums[i] == val) {
                continue;
            }
            nums[index] = nums[i];
            index++;

        }
        return index;
    }

}


