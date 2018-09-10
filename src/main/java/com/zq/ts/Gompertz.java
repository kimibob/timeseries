package com.zq.ts;

import java.util.ArrayList;
import java.util.List;

public class Gompertz {

	//分3组数据
	private static int group_num = 3;
	//数据(个数必须是3的整数倍)
	private static double[] data = new double[]{
//			8.05, 9.13, 8.63, 10.57, 11.35, 12.85, 12.67, 13.01, 13.54
			//10,9,8,7,6,5,4,3,2
			518.17 
			,518.01 
			,569.12 
			,582.70 
			,632.64 
			,648.78 
			,723.02 
			,765.00 
			,914.55 
			,880.91 
			,863.45 
			,911.93 
			,940.37 
			,938.67 
			,981.38 
			,1079.54 
			,1186.78 
			,1273.60 
			,1339.98 
			,1495.64 
			,1570.04 
			,1863.22 
			,2189.13 
			,2520.49 
			,3064.56 
			,3463.98 
			,3874.08 

	};
	//每组数据的个数
	private static int n = data.length/group_num;

	
	public static void main(String[] args) {
		ArrayList<Double> data_list = new ArrayList<Double>();
		for (int i = 0; i < data.length; i++) {
			data_list.add(Math.log(data[i]));
		}
		List<Double> data_group1_ln = data_list.subList(0, n);
		List<Double> data_group2_ln = data_list.subList(n, 2*n);
		List<Double> data_group3_ln = data_list.subList(2*n, 3*n);
		
		double data_group1_ln_sum = 0;
		double data_group2_ln_sum = 0;
		double data_group3_ln_sum = 0;
		for (int i = 0; i < n; i++) {
			data_group1_ln_sum += data_group1_ln.get(i);
			data_group2_ln_sum += data_group2_ln.get(i);
			data_group3_ln_sum += data_group3_ln.get(i);
		}
		System.out.println(data_group1_ln_sum+"-"+data_group2_ln_sum+"-"+data_group3_ln_sum);
		double b = Math.pow((data_group3_ln_sum - data_group2_ln_sum)
				/(data_group2_ln_sum - data_group1_ln_sum)
				, 1.0/n);
		double ln_a = (b-1)*(data_group2_ln_sum - data_group1_ln_sum)/(b*Math.pow((Math.pow(b, n)-1),2));
		System.out.println("ln_a:"+ln_a);
		double a = Math.exp(ln_a);
		
		double ln_k = (data_group1_ln_sum - (Math.pow(b, n)-1)*b*ln_a/(b-1))/n;
		double k = Math.exp(ln_k);
		System.out.println(k+","+a+","+b);
		for (int i = data.length+1; i <= data.length+184; i++) {
			System.out.println(getGompertz(k, a, b, i));
		}
	}
	
	public static double getGompertz(double k, double a, double b, int t) {
		return k*Math.pow(a, Math.pow(b, t));
	}
}
