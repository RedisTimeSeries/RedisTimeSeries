#!/bin/bash
VERBOSE=${VERBOSE:-0}
PATCH=../ryu.patch
DIRNAME=${DIRNAME:-"ryu"}
[[ $VERBOSE == 1 ]] && set -x
cd $DIRNAME

git apply --check $PATCH 2>/dev/null
#If the patch has not been applied then the $? which is the exit status
#for last command would have a success status code = 0
if [ $? -eq 0 ]; then
    if [ $VERBOSE -eq 1 ]; then
        echo "Applying patch $PATCH"
    fi
    git apply --verbose $PATCH
else

    if [ $VERBOSE -eq 1 ]; then
        echo "Patch $PATCH was already applied. No need to apply."
    fi
fi
