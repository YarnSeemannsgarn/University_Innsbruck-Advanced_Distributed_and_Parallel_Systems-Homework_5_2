#!/bin/bash                                                                                                                                                             

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR
${DIR}/povray +I${DIR}/scherk.pov +FN +W1024 +H768 +KFI$1 +KFF$2 +SF$3 +EF$4