#!/bin/bash
helm init --upgrade --service-account tiller
helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
helm repo upgrade
helm install --name my-kafka incubator/kafka

kubectl apply -f kafka.yml
kubectl apply -f benchmark.yml
