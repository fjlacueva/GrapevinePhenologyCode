> crontab -e
...
# Execute every minute
* * * * * /projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/crontab/copyAndDestroy.sh >> /logs/logCrontabCopyAndDestroy.log 2>&1 