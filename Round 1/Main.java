import java.util.*;
public class Main {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        String str = scan.nextLine();
        for(int i=0;i<str.length();i++)
        {
            int count=0;
            char check = str.charAt(i);
            for(int j=i+1;j<str.length();j++)
            {
                if(check==str.charAt(j))
                    count++;
            }
            if(count==0)
            {
                System.out.println(check);
                break;
            }
        }
        scan.close();
    }
}