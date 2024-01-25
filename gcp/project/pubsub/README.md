# Message Publishing to GCP PubSub
Script for optionally retrieving and publishing messages to GCP PubSub

# Prerequisites

Setup virtualenv Environment for the data_engineering repo from the top level README.md

Setup virtualenv Environment
```
python3 -m venv /path/to/new/virtual/environment
cd /path/to/new/virtual/environment
source bin/activate
```

Install Python libraries
```
pip install -r requirements.txt
```

# Use Cases

## Message Payloads in a Folder

You have all the message payloads as individual JSON/etc files inside a folder. Each payload is an individual file in the folder.

Script: publish_message_to_pubsub_topic.py

Parameters Required: 
--projectid <destination PubSub project>
--pubsub-topic <PubSub topic name>
--message-data-folder "<path to the folder containing the messages>"

Example:
Command Line:
```
./publish_message_to_pubsub_topic.py --projectid myproject --pubsub-topic RELEASE_UPDATES --message-data-folder "/Users/Lee/Documents/messages"