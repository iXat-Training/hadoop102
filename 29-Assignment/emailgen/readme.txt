A simple node script to write some fake data into /zktests znode. The script uses FakerJS to create some fake person names and phone numbers. It uses node-zookeeper-client node module to interact with ZK quorum. The script sets some random fake data once every 3 secs.

Steps ---
	Install Zookeeper
	Install Kafka
	Start ZK and Kafka
	Create a Topic with name EMAILIN
		  kafka-topics.sh --create --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --topic EMAILIN

	Install NodeJS
		If you are on CentOS 6.x ==>
				 rpm -ivh http://epel.mirror.net.in/epel/6/i386/epel-release-6-8.noarch.rpm
				yum install npm --enablerepo=epel
		test NodeJS
			type node, and in the node console type console.log("Done...")

	Once done open a new bash/cmd prompt and cd to this dir of the sample and do "npm install"

	run the node app by typing in
		 node randomemail.js

    the program would be sending random emails to the EMAILIN topic with a sleep interval of 1000ms.

    you can change the broker URL in line#5






