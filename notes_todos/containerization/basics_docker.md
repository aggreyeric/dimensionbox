# Notes

## Docker Overview [optional]
[Docker Overview](https://docs.docker.com/guides/docker-overview/)
[Docker Guides](https://docs.docker.com/guides/)

## Image and Container

- Images
An image is a read-only template with instructions on how to create a Docker container. using the FROM instruction on our dockerfile we can base our image on an existing image. we use the command docker build to create our image from our dockerfile

- Containers
A container is a runnable instance of an image. 



## Images Commands
docker images =  gives all images
docker pull mysql = pulls mysql image
docker rmi   <IMAGE ID>
docker rmi $(docker images -q) -f

## Containers
 docker ps # all containers that are running
 docker ps -a # All containers running and stopped
 docker ps -aq # All containers IDS only
 docker ps -l # last container
 docker ps # all containers that are running
 docker run -it ubuntu /bin/bash 
 docker exec -it <container id> /bin/bash
  docker exec -it b567826e71f4 /bin/bash
  docker stop <container id>
docker container stop $(docker container ls -aq)  # stop all containers
docker container prune -f  # remove all containers
 <!-- docker run an ubuntu os with bash shell opened in it -->

 # FROM 
FROM <image>
 Dockerfiles normally start with the FROM instruction. This pulls an image that will be used as the base layer for the image the Dockerfile will build

# LABEL 

Dockerfile creates a LABEL that specifies “nigelpoulton@hotmail.com” as the maintainer of the image. Labels are optional key-value pairs and are a good way of adding custom metadata. For example, you can add the maintainer of the image to your Dockerfile.