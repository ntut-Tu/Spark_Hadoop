
# Setup Steps

1. **Install Docker**
   Make sure Docker is installed and running properly on your system.
   > * We are using a prebuilt image, so we cannot customize startup commands to automatically setup with docker-compose.

2. **Run `(docker)setup.bat`**

   > Ensure the setup completes without any errors.
   > *Note:* On some systems (especially Windows 11), permission issues may occur. If that happens, you may need to manually install using the image instead.

3. **Run `(win)init_environment.bat`**
   > Some required HDFS directories must be created manually **from outside the container**.
   > This is because:
   > * The **master container** does **not** have HDFS installed or permission to access HDFS.
   
4. **Run `(win)run_spark.bat`**

   > This script creates the clustering model for prediction and saves it to HDFS.

5. **Run `(win)start_spark_stream.bat`**

   > This starts the Kafka streaming service.
   > 🖥 *Keep this window open* — closing it will stop the streaming process.

6. **Run `(win)run_test.bat`**

   > Runs a test to verify the installation.
   > ✅ If it completes successfully, the system has been set up correctly.

7. **Run `(win)setup_server.bat`**

   > * Starts the backend server. You can test the API via Postman at: [http://localhost:8001](http://localhost:8001)
   > * 🖥 *Keep this window open* — closing it will terminate the server. Restarting it later may be difficult.
   > * Once the server is running, the frontend should work properly.
   > * The frontend is accessible at: [http://localhost:3000](http://localhost:3000)

