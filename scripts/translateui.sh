#!/bin/sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 form.ui lang1 [lang2 ...]"
  exit 1
fi

if [ -z $(which uic-qt5) ] || [ -z $(which lupdate-qt5) ]; then
  echo "uic-qt5 and lupdate-qt5 need to exist in PATH"
  exit 1
fi


cd $(dirname $1)
ui=$(basename $1)

if [ ! -f "$ui" ]; then
  echo "$1 does not exist"
  exit 1
fi

shift

tsfiles () {
  for i in $@
  do
    echo ${ui/.ui/}_$i.ts
  done
}

uic-qt5 $ui > ui_${ui/.ui/.h}

cat > ${ui/.ui/.pro} <<EOF
SOURCES += ui_${ui/.ui/.h}
TRANSLATIONS += $(tsfiles $@)
EOF

lupdate-qt5 ${ui/.ui/.pro}

rm ui_${ui/.ui/.h}
rm ${ui/.ui/.pro}
