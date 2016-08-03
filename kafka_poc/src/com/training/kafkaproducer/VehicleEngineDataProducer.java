package com.training.kafkaproducer;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class VehicleEngineDataProducer 
{
	public static void genVehicleData(String fileLocation, String topicName) throws Exception {
		Properties props = null;
		Random random = null;
		ProducerConfig config = null;
		int engineCoolantTemp=0;
		int fuelPressure=0;
		int intakeManifoldAbsPress=0;
		int engineRPM=0;
		int vehicleSpeed=0;
		int intakeAirTemp=0;
		int airFlowRate=0;
		int pistonSpeed=0;
		int batteryVolt=0;
		StringBuffer sbfData=null;
		FileInputStream fin=null;
		BufferedReader br = null;
		int num=0;
		try {
			System.out.println("Going into Vehicle Data Producer");
			props = new Properties();
			props.put("metadata.broker.list", "aster1.com:9092");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("request.required.acks", "1");
			config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(config);
			random = new Random();
			sbfData = new StringBuffer();
			fin = new FileInputStream(fileLocation);
			br = new BufferedReader(new InputStreamReader(fin));
			for(String strLine; (strLine = br.readLine()) != null; ) {
						System.out.println("I'm in loop");
					engineCoolantTemp = random.nextInt((315) - (-50)) + (-50);
					fuelPressure = random.nextInt((865) - (-10)) + (-10);
					intakeManifoldAbsPress = random.nextInt((355) - (-10)) + (-10);
					engineRPM = random.nextInt(17383);
					vehicleSpeed = random.nextInt(355);
					intakeAirTemp = random.nextInt((315) - (-50)) + (-50);
					airFlowRate = random.nextInt(755);
					pistonSpeed = random.nextInt(20);
					batteryVolt = random.nextInt((20) - (6)) + (6);
					sbfData.append(strLine).append(",");
					sbfData.append(engineCoolantTemp).append(",");
					sbfData.append(fuelPressure).append(",");
					sbfData.append(intakeManifoldAbsPress).append(",");
					sbfData.append(engineRPM).append(",");
					sbfData.append(vehicleSpeed).append(",");
					sbfData.append(intakeAirTemp).append(",");
					sbfData.append(airFlowRate).append(",");
					sbfData.append(pistonSpeed).append(",");
					sbfData.append(batteryVolt).append("\n");
									}
				System.out.println("Whole generated Vehicle Engine data: \n" + sbfData.toString());
				num++;
				System.out.println("Number : "+num);
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, sbfData.toString());
				producer.send(data);
			
			producer.close();
			System.out.println("Produced data");
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	public static void main(String[] args) {
		String fileLoc = "";
		String topicName="";
		try {
			System.out.println("Welcome");
	        fileLoc=args[0];
	        topicName=args[1];
	        VehicleEngineDataProducer.genVehicleData(fileLoc,topicName);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}