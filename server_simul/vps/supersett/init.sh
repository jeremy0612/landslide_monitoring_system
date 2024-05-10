#!/bin/bash
superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password 123456

superset db upgrade
superset init