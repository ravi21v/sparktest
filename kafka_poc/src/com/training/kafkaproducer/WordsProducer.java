package com.training.kafkaproducer;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileWriter;

public class WordsProducer 
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
                                //File fileRead=null;
                                FileInputStream fin=null;
                                BufferedReader br = null;
                                FileWriter outFileStr = null;
                                try {
                                                System.out.println("Going into main class");
                                                props = new Properties();
                                                props.put("metadata.broker.list", "aster1.com:9092");
                                                props.put("serializer.class", "kafka.serializer.StringEncoder");
                                                props.put("request.required.acks", "1");
                                                random = new Random();
                                                sbfData = new StringBuffer();
                                                fin = new FileInputStream(fileLocation);
                                                br = new BufferedReader(new InputStreamReader(fin));
                                                for(String strLine; (strLine = br.readLine()) != null; ) {
                                                                //System.out.println (strLine);
                                                                //int randomInteger = random.nextInt(765);
                                                                //engineCoolantTemp = random.nextInt(upperBound - lowerBound) + lowerBound;
                                                                engineCoolantTemp = random.nextInt((315) - (-50)) + (-50);
                                                                fuelPressure = random.nextInt((865) - (-10)) + (-10);
                                                                intakeManifoldAbsPress = random.nextInt((355) - (-10)) + (-10);
                                                                engineRPM = random.nextInt(17383);
                                                                vehicleSpeed = random.nextInt(355);
                                                                intakeAirTemp = random.nextInt((315) - (-50)) + (-50);
                                                                airFlowRate = random.nextInt(755);
                                                                pistonSpeed = random.nextInt(20);
                                                                batteryVolt = random.nextInt((20) - (6)) + (6);
                                                                //System.out.println("Engine coolant Temperature: " + engineCoolantTemp);
                                                                //System.out.println("Fuel Pressure: " + fuelPressure);
                                                                //System.out.println("Intake Manifold Absolute Pressure: " + intakeManifoldAbsPress);
                                                                //System.out.println("Enigne RPM: " + engineRPM);
                                                                //System.out.println("Vehicle Speed: " + vehicleSpeed);
                                                                //System.out.println("Intake Air Temperature: " + intakeAirTemp);
                                                                //System.out.println("MAF AirFlow Rate: " + airFlowRate);
                                                                //System.out.println("Piston Speed: " + pistonSpeed);
                                                                //System.out.println("Battery Voltage: " + batteryVolt);
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
                                                                //Float randomInteger1 = random.nextFloat();
                                                                //double randomDouble = Math.random();
                                                                //System.out.println("Random Number in Java: " + randomInteger1);
                                                }
                                                System.out.println("Whole generated Vehicle Engine data: \n" + sbfData.toString());
                                                outFileStr = new FileWriter("/home/cts472740/Output/test_out.txt");
                                                outFileStr.write(sbfData.toString());
                                                outFileStr.close();
                                                config = new ProducerConfig(props);
                                                Producer<String, String> prod = new Producer<String, String>(config);
                                                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, sbfData.toString());
                                                prod.send(data);
                                                System.out.println("Produced data");
                                                prod.close();
                                } catch (Exception e) {
                                                System.out.println(e);
                                }finally{
                                                outFileStr.close();
                                }
                }
                public static void main(String[] args) {
                                String fileLoc = "";
                                String topicName="";
                                try {
                                                System.out.println("Welcome");
                                                //for(int i = 0; i < args.length; i++) {
                        //    System.out.println(args[i]);
                        fileLoc=args[0];
                        topicName=args[1];
                        //}
                                                WordsProducer.genVehicleData(fileLoc,topicName);
                                } catch (Exception e) {
                                                System.out.println(e);
                                }
                }
}
