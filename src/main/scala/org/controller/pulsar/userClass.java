package org.controller.pulsar;


public class userClass {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public userClass(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public userClass() {
    }

    public static userClass builder() {
       return new  userClass() ;
    }

    public userClass name(String name){
        this.name=name;
        return this;
    }

    public userClass age(int age){
        this.age=age;
        return this;
    }

    public userClass build(){
        return this;
    }

    String name;
    int age;
}