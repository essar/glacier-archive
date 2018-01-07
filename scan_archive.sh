#!/bin/bash

#
# Scans the archive directory and generates increment TARs

RUNDATE=$(date +%Y%m%d%H%M)
PWD=$(pwd)

ARCHIVEDIR=/home/sroberts/backup_scripts/glacier/archive
BACKUPDIR=/home/sroberts/backup_scripts/glacier/backup/run-$RUNDATE
LOGSDIR=/home/sroberts/backup_scripts/glacier/logs

echo PWD=$PWD
echo ARCHIVEDIR=$ARCHIVEDIR
echo BACKUPDIR=$BACKUPDIR

# Import functions
. ./build_increment.sh

#Move relative to archive directory
cd $ARCHIVEDIR

for d in `find $ARCHIVEDIR/* -maxdepth 0 -type d`; do

  echo Scanning $d
  build_increment $d $LOGSDIR $BACKUPDIR

done


