# Spam-Stack
> Enhancing User Experience by Increasing the Content Quality of Stack Overflow

## Overview
Programmer's take to Stack Overflow to find out solutions to a large number of quite simple questions that come across as hurdles to progressing their understanding and fixing software issues. Lot of time is often spent on scrolling through irrelevant posts till we find the correct answer. Spam Stack is here to solve this problem and helps you, reach their answers faster by clearing out all the irrelevant posts and increasing the content quality of the parent posts.
You can access the interactive webiste here : [datasavvyguy.me](http://www.datasavvyguy.me/)

## Pipeline
![Pipeline](https://github.com/Omkaar23/stackoverflow-analytics/blob/develop/assets/pipeline.PNG)

## Requirements
* Python
* [AWS CLI](https://aws.amazon.com/cli/)
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation)

## Architecture
* [SPARK](https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88): 4 EC2 m4.large instances with (1 master 3 worker) spark cluster
* [POSTGRESQL](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252): 1 EC2 m5a.large instance
* [DASH](https://dash.plot.ly/installation): 1 EC2 t2.micro instance 

## DashBoard
![DashBoard](https://github.com/Omkaar23/stackoverflow-analytics/blob/develop/assets/frontend.PNG)


