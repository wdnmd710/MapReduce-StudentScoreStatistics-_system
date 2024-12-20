package StudentScore;

import StudentScore.ProvinceScore.ProvinceScoreDriver;
import StudentScore.StudentTotalScore.TotalScoreDriver;
import StudentScore.Top100.Top100Driver;
import StudentScore.finalExam4.SubjectAverageDriver;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Scanner;

/**
 * @Author: yuan
 */
public class menu {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Scanner sc = new Scanner(System.in);
        while(true){
            System.out.println("=========基于MapReduce的学生成绩分析=========");
            System.out.println("1.全国学生总分降序排序");
            System.out.println("2.各省总分降序排序");
            System.out.println("3.各科成绩前一百名");
            System.out.println("4.各科成绩平均分");
            System.out.println("5.退出");
            System.out.print("请输入功能选项：");
            int option = sc.nextInt();
            Method method=null;
            switch (option){
                case 1: method= TotalScoreDriver.class.getMethod("main", String[].class);
                    method.invoke(null,(Object) new String[] {});
                    break;
                case 2: method= ProvinceScoreDriver.class.getMethod("main", String[].class);
                    method.invoke(null,(Object) new String[] {});
                    break;
                case 3: method= Top100Driver.class.getMethod("main", String[].class);
                    method.invoke(null,(Object) new String[] {});
                    break;
                case 4: method= SubjectAverageDriver.class.getMethod("main", String[].class);
                    method.invoke(null,(Object) new String[] {});
                    break;
                case 5:
                    System.exit(1);
                    break;
                default:
                    System.out.println("输入正确选项！！");
                    break;
            }
        }
    }
}
