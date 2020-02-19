#!/usr/bin/env bash

# Prerequisite check for docker service
if [ ! "$(systemctl is-active docker)" = "active" ];
then
    echo "You need to start Docker first (possibly sudo systemctl start docker.service)"
    exit
fi

mkdir -p data
cd data

echo "Fetching latest danish dataset checksum..."
wget https://download.geofabrik.de/europe/denmark-latest.osm.pbf.md5 -O denmark-latest.osm.pbf.md5 -q --show-progress

echo "Comparing checksums..."
if ! md5sum -c denmark-latest.osm.pbf.md5;
then
    echo "Downloading danish dataset..."
    wget https://download.geofabrik.de/europe/denmark-latest.osm.pbf -O denmark-latest.osm.pbf -q --show-progress
    
    echo "Stop running docker container"
    docker stop osrm-denmark-latest

    echo "Removing outdated docker container.."
    docker rm osrm-denmark-latest
fi

if [ ! "$(docker ps -a | grep osrm-denmark-latest)" ];
then
    echo "Extracting graph of OSM..."
    docker run --name osrm-denmark-latest-extract -t -v "${PWD}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/denmark-latest.osm.pbf
    docker rm osrm-denmark-latest-extract

    echo "Partitioning graph to cells..."
    docker run --name osrm-denmark-latest-partition -t -v "${PWD}:/data" osrm/osrm-backend osrm-partition /data/denmark-latest.osrm
    docker rm osrm-denmark-latest-partition

    echo "Calculating routing weights for cells..."
    docker run --name osrm-denmark-latest-customize -t -v "${PWD}:/data" osrm/osrm-backend osrm-customize /data/denmark-latest.osrm
    docker rm osrm-denmark-latest-customize

    docker run --name osrm-denmark-latest -d -t -i -p 5000:5000 -v "${PWD}:/data" osrm/osrm-backend osrm-routed --max-viaroute-size 100000 --max-matching-size 100000 --algorithm mld /data/denmark-latest.osrm 
else
    docker start osrm-denmark-latest
fi

cd ..
