package org.controller.pulsar;


public class tmpUserCool {

    String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    Integer age;
    tmpUserCool(String name,Integer age){
        this.age=age;
        this.name=name;
    }
    tmpUserCool(){}
    byte[] parse(){
        String returnURL="{\"name\":"+this.name+",\"age\":"+this.age+"}}";
        return returnURL.getBytes();
    }

}

