#! /bin/bash

docker build -f Dockerfile -t scottmlittle/toll_prediction:r7.0.5 .

if [ "$?" -eq "0" ]
then
	echo "Docker Build Successful, Publishing container publicly"
	sudo docker push scottmlittle/toll_prediction:r7.0.5
else
 	echo "Docker Build Failed, no release executed"
fi
