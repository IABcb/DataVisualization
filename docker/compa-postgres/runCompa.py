#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
os.system("sudo docker stop postgresql")
os.system("sudo docker rm postgresql")
os.system("sudo docker build -t compa/postgres:DEMO .")
os.system("sudo docker run --name postgresql -itd --restart always --publish 80:8080 --publish 5432:5432 --volume /srv/docker/postgresql:/var/lib/postgresql compa/postgres:DEMO")


