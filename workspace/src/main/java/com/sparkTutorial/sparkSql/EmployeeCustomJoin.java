package com.sparkTutorial.sparkSql;

/**
 * Created by Neha Priya on 02-10-2019.
 */
public class EmployeeCustomJoin {

    private String first_name;
    private Integer dept_no;

    public EmployeeCustomJoin(String first_name, Integer dept_no) {
        this.first_name = first_name;
        this.dept_no = dept_no;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "first_name='" + first_name + '\'' +
                ", dept_no=" + dept_no +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmployeeCustomJoin employee = (EmployeeCustomJoin) o;

        if (!first_name.equals(employee.first_name)) return false;
        return dept_no.equals(employee.dept_no);
    }

    @Override
    public int hashCode() {
        int result = first_name.hashCode();
        result = 31 * result + dept_no.hashCode();
        return result;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public Integer getDept_no() {
        return dept_no;
    }

    public void setDept_no(Integer dept_no) {
        this.dept_no = dept_no;
    }
}
