#!/bin/bash
PYTHONENV=/python/.envs/dataPresentationEnv
PYTHONCODE=/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/crontab
DATAOUTPUT=/projects/grapevine/GIT/src/data/dataPresentation
LOGSPATH=/projects/grapevine/GIT/src/logs

for f in $DATAOUTPUT/*; do
   if test -f "$f"; then
      echo Copying $f...
      $PYTHONENV/bin/python $PYTHONCODE/copyAndDestroy.py $f >> $LOGSPATH/logCopyAndDestroy.log
      rc=$?
      if [ ${rc} -eq 0 ]; then
         echo "Success copyAndDestroy.rc:$rc --> deleting $f"
         yes | rm $f
      else
         echo "Failure copyAndDestroy.rc:$rc for file $f!!"
      fi
   fi
done