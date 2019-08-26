package com.company.fileprocessingengine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.company.fileprocessingengine.business.FileProcessor;

/**
 * 
 * @author Yash Khatri
 * It is the main class. The starting point of this commandline
 * application.
 */
@SpringBootApplication
public class Application  implements org.springframework.boot.CommandLineRunner
{
	@Autowired
	FileProcessor fileProcessor;
	
	
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(Application.class, args);
		
	}

	@Override
	public void run(String... args) throws Exception {
		//Running this application for ever.
		while(true)
		fileProcessor.readFiles();	
	}
}
