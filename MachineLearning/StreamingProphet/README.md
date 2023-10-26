# Streaming Prophet Production Models 

Run this demo on a fixed node cluster running DBR 13.3LTS ML. Attach the appropriate IAM role for authentication purposes. 

This demo will stream incoming data from MSK, provide a forecast, then output to Kafka topic an a delta lake table. The consumer can read from kafka or from Delta Lake. 




