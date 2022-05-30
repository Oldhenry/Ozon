using TestClasses;

public static class Program
{
    public delegate int MyDelegate(int value);
    
    public static void Main(string[] args)
    {
        var array = new int[5] { 1, 2, 3, 4, 5 };
        //Console.WriteLine(array[9]);
        
         Action[] actions = new Action[3];
         int idx = 0;
         foreach (char ch in "abc")
             actions[idx++] = () => Console.Write(ch);
        
         foreach (Action ac in actions)
             ac(); // abc // ccc in C# 4.0
        
         Console.WriteLine();
        
         var lst = new List<Func<int>>();
        
         for (int i = 0; i < 10; i++)
         {
             lst.Add(() => MyFunc(i));
         }
        
         for (int i2 = 0; i2 < 5; i2++)
         {
             Console.WriteLine(lst[i2].Invoke());
        
         }
        
        
         var lst2 = new List<Func<int>>();
         for (int i = 0; i < 5; i++)
         {
             var k = i;
             var f = new MyDelegate(MyFunc);
             lst2.Add(() => f(k));
             k++;
         }
        
         foreach (var d in lst2)
         {
             d();
         }
        
         var lst3 = new List<Func<int>>();
         for (int i = 0; i < 5; i++)
         {
             var k = i;
             var f = new MyDelegate(MyFunc);
             lst3.Add(() => f(k));
             k++;
         }
        
         foreach (var d in lst3)
         {
             d();
         }
        
         for (int i2 = 0; i2 < 5; i2++)
         {
             Console.WriteLine(lst2[i2].Invoke());
        
         }
        
        
         var g = int.MaxValue;
         var t=++g;
         var t1=g++;
        
        //ClassA a = new ClassA();
        
        ClassB b1 = new ClassB(24);
        
        ClassB b = new ClassB();
        ClassB b2 = new ClassB(29, "boss");
        
        ClassC c = new ClassC();

        ClassA a = new ClassA();
        
        a.Method();
        b.Method();
        c.Method();

        a = c;
        a.Method();
        
        a = b;
        a.Method();

        b = c;
        b.Method();
        
        //return Task.Run(() => null);
    }
    
    static int MyFunc(int value)
    {
        Console.WriteLine($"Value={value}");
        return value;
    }
}



