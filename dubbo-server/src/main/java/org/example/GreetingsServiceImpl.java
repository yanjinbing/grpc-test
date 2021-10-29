package org.example;

public class GreetingsServiceImpl implements GreetingsService{
    @Override
    public String sayHi(String name) {
        return "hi, " + name;
    }
}
