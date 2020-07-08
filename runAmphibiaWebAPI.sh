source /home/jdeck/.bashrc
cd /home/jdeck/code/AmphibiaWebDiseasePortalAPI
git pull
/usr/bin/sudo -u jdeck -i python /home/jdeck/code/AmphibiaWebDiseasePortalAPI/fetch.py
git add -A
git commit -m "updating based on automatic fetch process"
git push
