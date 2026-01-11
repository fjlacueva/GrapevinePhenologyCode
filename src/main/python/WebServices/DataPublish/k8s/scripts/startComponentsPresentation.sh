#!/bin/bash
NSPACE=$1
COMPONENTSPATH='./..'

kubectl -n $NSPACE delete -f $COMPONENTSPATH/componentsPublish.yml
sleep 1
kubectl -n $NSPACE apply  -f $COMPONENTSPATH/componentsPublish.yml