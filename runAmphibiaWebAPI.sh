#!/bin/bash
source /home/jdeck/.bashrc
cd /home/jdeck/code/AmphibiaWebDiseasePortalAPI
git pull
#for some reason, i was experimenting with running python script as sudo, i forget why
#however, when running sudo using -i command we reset the $HOME directory to /root
#and running commands as sudo can be tricky:
#https://ubuntuforums.org/showthread.php?t=983645&s=fb7898eaa14d3421dae0381af1a0d3e6&p=6188826#post6188826
#/usr/bin/sudo -u jdeck -i python /home/jdeck/code/AmphibiaWebDiseasePortalAPI/fetch.py
# instead, i am reverting to running as the current user
python3 /home/jdeck/code/AmphibiaWebDiseasePortalAPI/fetch.py
git add -A
git commit -m "updating based on automatic fetch process"
git push

gunzip -c -f data/amphibian_disease_data_processed.csv.gz > data/amphibian_disease_data_processed.csv

#TODO: add loader.py script once stable here
