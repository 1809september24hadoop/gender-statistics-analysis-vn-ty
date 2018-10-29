package com.revature;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Reader {
	public static void main(String[] args) {
		String line = "";
		try(BufferedReader br = new BufferedReader(new FileReader("/home/cloudera/Documents/Gender_StatsData.csv"))) {
			line = br.readLine();
			String[] row = line.split(",");
			for(int i = 0; i < row.length; i++) {
				System.out.println("Index: " + i + ";;; Data: " + row[i]);
			}
//			while((line = br.readLine()) != null) {
//				String[] row = line.split(",");
//				System.out.println(row[3] + " : " + row[4]);
//			}
		} catch (FileNotFoundException e) {
			System.out.println("FILE NOT FOUND!!!");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
