# Kafka Admin operations


  The Kafka Admin API supports managing and inspecting topics, brokers, and other Kafka objects.
In this project using the Kafka Admin API, exposed the below REST end points for creating,describing and deleting Topics in the kafka cluster.


* POST              ```	/v1/topic	```	  				- For creating a topic into Kafka Cluster.<br />
* DELETE            ```/v1/topic	```		  				- For deleting a particular topic in the kafka cluster.<br />
* GET               ```/v1/topic/{topicName} ```	- For Describing a particular topic in the kafka cluster.<br />
* GET               ```/v1/topics	```						- For Listing all the topics in the kafka cluster.<br />




