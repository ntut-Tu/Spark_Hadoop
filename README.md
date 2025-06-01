# Setup Step
1. install docker
2. run (docker)setup.bat
    > Make sure no errors occur during the setup process. In some environments (especially win11), permission issues may arise; in such cases, you may need to install using the image directly instead. 
3. run (win)init_environment.bat
4. run (win)run_spark.bat
5. run (win)start_spark_stream.bat  
   > This windows need to be kept open for the kafka streaming to run.
6. run (win)run_test.bat 
   > To test the installation. If it runs successfully, the installation is complete.
7. run (win)setup_server.bat
   > - This will start the server and you can test it with postman at http://localhost:8001  
   > - This windows also need to be kept open for the server to run, or else it'll be difficult to turn off / restart after.
   > - Now on your frontend should be able to run normally.
   > - frontend is available at http://localhost:3000