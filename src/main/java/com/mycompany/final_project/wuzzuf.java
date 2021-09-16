/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.final_project;

/**
 *
 * @author aayma
 */
public class wuzzuf {
    private String company;
    private Integer count;

    public String getCompany() {
        return company;
    }

    public Integer getCount() {
        return count;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public void setCount(Integer age) {
        this.count = age;
    }

    @Override
    public String toString() {
        return "wuzzuf{" + "company=" + company + ", count=" + count + '}';
    }
    
    
}

