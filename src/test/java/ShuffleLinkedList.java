import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

public class ShuffleLinkedList {
    public static void main(String[] args) {
        // 假设 tupleList 是一个 LinkedList 数组，数组的每个元素是一个 LinkedList
        LinkedList<InputTuple<Integer, Integer, String>>[] tupleList = new LinkedList[10];

        // 初始化每个 LinkedList 并添加示例数据（这里只是举个例子）
        for (int i = 0; i < tupleList.length; i++) {
            tupleList[i] = new LinkedList<>();
            tupleList[i].add(new InputTuple<>(i, i * 2, "Example " + i));
        }

        // 对每个 LinkedList 进行打乱操作
        Random random = new Random();
        for (LinkedList<InputTuple<Integer, Integer, String>> linkedList : tupleList) {
            Collections.shuffle(linkedList, random);
        }

        // 打印打乱后的结果
        for (int i = 0; i < tupleList.length; i++) {
            System.out.println("LinkedList " + i + ": " + tupleList[i]);
        }
    }
}

// 输入元组类的定义
class InputTuple<T1, T2, T3> {
    T1 first;
    T2 second;
    T3 third;

    public InputTuple(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ", " + third + ")";
    }
}
