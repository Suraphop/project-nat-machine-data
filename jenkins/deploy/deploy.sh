#!/bin/bash

echo "***************************"
echo "** deploy to production v.1.0.0**"
echo "***************************"

IMAGE="tb-mcstatus"

sshpass -p $PRODUCTION_SERVER_PASSWORD ssh -o StrictHostKeyChecking=no production@10.128.16.210 << ENDSSH
    docker login -u devopsmic -p $DOCKER_PASSWORD
    docker stop $IMAGE || true && docker rm $IMAGE || true
    docker pull devopsmic/$IMAGE:$BUILD_TAG
    docker run --restart=always -d -v /home/production/ftp/data/mc_status/TB/:/app/data/data_mc_status/ --name mic/$IMAGE devopsmic/$IMAGE:$BUILD_TAG 
ENDSSH