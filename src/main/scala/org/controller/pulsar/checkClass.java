package org.controller.pulsar;


import java.nio.charset.StandardCharsets;

public class checkClass {

    public static int capitalStart=(int)'A';
    public static int smallStart=(int)'a';
    public static int alphabetSize=26;

    private static byte[] tail(byte[] inputChars){
        int size = inputChars.length;
        byte[] opArr = new byte[size-1];

        for (int i=1;i<size; i++)
            opArr[i - 1] = inputChars[i];

        return opArr;
    }

    private static String capitalize(String input){
        byte[] inChars=input.getBytes();
        String OpString= input;
        int startByte=(int)inChars[0] ;
        char[] tmpArr=new char[]{(char) (capitalStart + Math.abs(smallStart - startByte))};

        if(startByte >= smallStart && startByte <= (smallStart +alphabetSize))
            OpString= new String(tmpArr)  + new String(tail(inChars), StandardCharsets.UTF_8); // ,  StandardCharsets. UTF_8

        return OpString;
    }

    public static void main(String[] args){
        System.out.println(capitalize("data"));
    }
}
