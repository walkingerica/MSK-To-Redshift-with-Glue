The sample is for consuming data from MSK and writing to Redshift, including nested json processing.


## Data Producer  
1. Install dependency
   ```
   pip install -r data-producer-script/requirements.txt
   ```
3. Run code to send, once per second, a JSON message with sensor data to the Kafka topic.  
```
cd data-producer-script
python data-producer-script/kafka-producer.py
```
## Data Consumer 
1. Create a MSK connection with Network connection 
2. Create a Glue Job 
3. Update the Glue's Script, please replace your MSK broker and Redshift's access information



