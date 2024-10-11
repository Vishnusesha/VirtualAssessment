package Rotating;
import java.util.Scanner;
public class RotatingArray {
	public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
        System.out.println("Enter the size of the array:");
        int size=sc.nextInt();
        int[] arr=new int[size];
        System.out.println("Enter the elements:");
        for(int i=0;i<size;i++){
            arr[i]=sc.nextInt();
        }
        System.out.println("Enter the position that the array can rotate:");
        int k=sc.nextInt();
        int temp[]=new int[size];
        for(int i=size-k,j=0;i<size;i++,j++){
            temp[j]=arr[i];
        }
        for(int i=0,j=k;i<size-k;i++,j++){
            temp[j]=arr[i];
        }
        for(int i=0;i<size;i++){
            arr[i]=temp[i];
        }
        System.out.println("The rotated array is:");
        for(int i=0;i<size;i++){
            System.out.println(arr[i]);
        }
    }
}
