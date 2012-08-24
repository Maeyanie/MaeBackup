#!/bin/sh

DIR=$(dirname $0)

exec java -classpath "$DIR:$DIR/lib:$DIR/lib/*" maebackup.MaeBackup "$@"
