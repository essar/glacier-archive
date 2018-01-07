#!/bin/bash

#
# Script walks a given directory and builds a tarball of all changed files.

function usage() {

  echo "Usage: build_increment source_dir [refs_dir]
    source_dir   directory to scan for changes
    refs_dir     directory containing changelogs
    tars_dir     director to store incremental TAR files
  "

}

function build_increment() {

  # Count cmd args
  if [ -z $1 ]; then
    echo Missing arguments!
    echo $(usage)
    exit 1
  fi

  RUNDATE=$(date +%Y%m%d%H%M)
  SOURCEDIR=$1
  SOURCE=$(basename $SOURCEDIR)
  LOGDIR=${2:-$.}
  LOG=$LOGDIR/$SOURCE.tab
  NEWLOG=$LOG.new
  TARDIR=${3:-.}
  TAR=$TARDIR/backup-$SOURCE-$RUNDATE.tar

  # Check source existsi
  if [ ! -e $SOURCEDIR ]; then
    echo Cannot locate source directory!
    exit 1
  fi

  # Create the tab file if it doesn't exist
  if [ -e $LOG ]; then
    LASTBACKUP=`stat -c %Y $LOG`
  else
    touch $LOG
    LASTBACKUP=0
  fi

  changes=0

  for f in `find $SOURCEDIR -type f`; do

    fileSize=`stat -c %s $f`
    modTime=`stat -c %Y $f`
    fileHash=`echo $f $fileSize $modTime | sha1sum`
  
    if ! grep -q "$fileHash" $LOG; then

      if [ -e $TARDIR ]; then
        # Create backup directory if doesn't exist
        mkdir -p $TARDIR
      fi

      # Add file to TAR
      tar -rf $TAR $f 2> /dev/null

      # Increment change counter
      ((changes++))

    fi

    # Add every hash to new log
      echo "$fileHash" >> $NEWLOG

  done

  echo $changes file changes

  # Replace old log with new log
  mv $LOG $LOG.bak
  mv $NEWLOG $LOG


  if [[ $changes -gt 0 ]]; then

    # Verify file count
    if [[ $changes -ne `tar -tf $TAR | wc -l` ]]; then
      echo TAR file count mismatch!
      exit 1
    fi

    # Add the tab file to the archive
    tar -rf $TAR $LOG 2> /dev/null

  fi
}

