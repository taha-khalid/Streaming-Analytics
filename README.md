Streaming Analytics on Cloud Telemetry Data (Local Sandbox)This project implements a real-time streaming analytics pipeline on a local Windows machine using the Azure 2019 Public Dataset V2. It replays historical VM CPU utilization metrics as a real-time data stream, ingests them via a lightweight message broker, and processes them dynamically using rolling time windows.🛠️ System Requirements & Version MatrixTo run this pipeline locally without overloading your laptop, ensure your environment matches these specifications:ComponentTechnologyVersion / VariantOSWindows 10 / 1164-bitContainer EngineDocker DesktopLatest StableLanguage RuntimePython3.11 or 3.12Java RuntimeEclipse Temurin JDK17 (Required for Spark compatibility)Message BrokerRedpanda (Kafka API)Containerized (v23.2.1 or later)Stream EngineApache PySpark4.1.2 (Utilizing Scala 2.13)Windows HelperHadoop Winutils3.3.0🚀 Step-by-Step Setup Guide1. Install Java 17 (OpenJDK)Download the Windows x64 MSI installer from Adoptium Temurin JDK 17.Run the installer. CRUCIAL: Enable the option "Set JAVA_HOME environment variable" during installation.2. Configure Hadoop Environment Patch (winutils)Windows requires a specific binary helper to mimic Linux file permissions for Spark checkpoints.Create a directory structure on your system: C:\hadoop\bin.Download winutils.exe for Hadoop 3.3.0 from a trusted repository (e.g., cdarlint/winutils GitHub).Save winutils.exe directly inside C:\hadoop\bin.Open your Windows Environment Variables and create a new System Variable:Variable Name: HADOOP_HOMEVariable Value: C:\hadoopEdit your system Path variable and add a new entry: %HADOOP_HOME%\bin.3. Spin Up Ingestion InfrastructureMake sure Docker Desktop is running, then open a terminal in your project directory and execute:Bashdocker-compose up -d
This starts Redpanda (listening on port 19092) and Grafana (listening on port 3000) in detached background mode.4. Initialize Python Virtual EnvironmentOpen a fresh Windows PowerShell or Command Prompt window (to register the new environment variables) and run:PowerShell# Create virtual environment
python -m venv venv

# Activate virtual environment

.\venv\Scripts\activate

# Install dependencies

pip install kafka-python-ng pyspark==4.1.2
🏃‍♂️ Running the PipelineTo run the pipeline, you will need two separate terminals open with your virtual environment activated (.\venv\Scripts\activate).Phase A: Start the Stream Processing EngineIn Terminal 1, start the PySpark consumer.Bashpython processor.py
Note: On its very first execution, Spark will take 1–2 minutes to automatically download the required spark-sql-kafka-0-10_2.13:4.1.2 dependency package from Maven central.Phase B: Launch the Telemetry Stream ReplayerOnce the Spark engine indicates it is live and listening, switch to Terminal 2 and start streaming the telemetry data:Bashpython producer.py
🗂️ Data Pipeline Flow[Azure .csv.gz File]
│
▼ (producer.py via gzip streaming)
[Redpanda Topic: 'telemetry-stream']
│
▼ (processor.py via PySpark 4.1.2 Structured Streaming)
[Console Out: 30-Second Rolling Windows / Avg CPU]
🛑 Troubleshooting Quick-FixesJAVA_GATEWAY_EXITED Error: Your system is likely using an unsupported Java version (like Java 21 or 24). Verify your active version using java -version and point your JAVA_HOME environment variable explicitly to Java 17.HADOOP_HOME ... unset Error: Your system cannot find winutils.exe. Double-check that the file exists exactly at C:\hadoop\bin\winutils.exe and that you restarted your terminal after applying system environment variables.NoSuchMethodError (Scala/Kafka): Ensure you are initializing the Spark Session using the \_2.13 Scala artifact, as PySpark 4.x deprecates the older Scala \_2.12 packages.
