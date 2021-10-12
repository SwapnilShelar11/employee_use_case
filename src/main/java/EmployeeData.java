
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;


public class EmployeeData {
    //Method gives phone number of employee
    public void individualPhoneNumber(JavaRDD<String[]> employees, String name) {
        employees.filter(x -> x[1].equals("Donald")).foreach(y -> System.out.println("Name of employee is " + y[1] + " havine phone no. is " + y[4]));
    }
    //method give phone numbers of top 5 employees in table
    public void phoneNumbers(JavaRDD<String[]> employees){
        employees.take(5).forEach(y -> System.out.println("Name of employee is " + y[1] + " havine phone no. is " + y[4]));
    }
    //method gives list of employees whose salary greater than mentioned
    public void greaterSalaryEmployees(JavaRDD<String[]> employees,Double salary){
        employees.filter(x->Double.parseDouble(x[7])>salary).foreach(y->System.out.println(y[1]+" having salary "+y[7]));
    }
    //method gives manager id of employee
    public void managerId(JavaRDD<String[]> employees,String empName){
        employees.filter(x->x[1].equals(empName)).foreach(y -> System.out.println("Name of employee is " + y[1] + " ,manager Id of him is " + y[9]));
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> line = sc.textFile("C:\\fireworks\\employee_use_cases\\src\\main\\java\\employees.csv");

        String header = line.first();
        String[] headerCol = header.split(",");
        System.out.println("Heading of tables...");
        for (int i = 0; i < headerCol.length; i++) {
            System.out.print(headerCol[i]+" ,");
        }
        System.out.println();
        System.out.println("----------------------------------------------");
        JavaRDD<String> employeeData = line.filter(x -> !x.contains(header));
        JavaRDD<String[]> eachEmployee = employeeData.map(x -> x.split(","));

        EmployeeData objFileRDD = new EmployeeData();
        System.out.println("Retrieve phone number of Donald...");
        objFileRDD.individualPhoneNumber(eachEmployee, "Donald");
        System.out.println("----------------------------------------------");

        System.out.println("Retrieve phone numbers of Top 5 employees...");
        objFileRDD.phoneNumbers(eachEmployee);
        System.out.println("----------------------------------------------");

        System.out.println("List employee who's salary greater than 1000...");
        objFileRDD.greaterSalaryEmployees(eachEmployee,10000.0);
        System.out.println("----------------------------------------------");

        System.out.println("Manager Id of Hermann is...");
        objFileRDD.managerId(eachEmployee,"Hermann");
        System.out.println("----------------------------------------------");
    }

}
