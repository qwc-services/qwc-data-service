#!/bin/sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 form.ui lang1 [lang2 ...]"
  exit 1
fi

UIC=""
LUPDATE=""
if [ -x $(which uic 2>/dev/null) ] && [ -x $(which lupdate 2>/dev/null) ]; then
  UIC="uic"
  LUPDATE="lupdate"
elif [ -x $(which uic-qt5 2>/dev/null) ] && [ -x $(which lupdate-qt5 2>/dev/null) ]; then
  UIC="uic-qt5"
  LUPDATE="lupdate-qt5"
elif [ -x $(which uic-qt6 2>/dev/null) ] && [ -x $(which lupdate-qt6 2>/dev/null) ]; then
  UIC="uic-qt6"
  LUPDATE="lupdate-qt6"
else
  echo "Could not find uic and/or lupdate in PATH"
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

$UIC $ui > ui_${ui/.ui/.h}

cat > ${ui/.ui/.pro} <<EOF
SOURCES += ui_${ui/.ui/.h}
TRANSLATIONS += $(tsfiles $@)
EOF

$LUPDATE ${ui/.ui/.pro}

rm ui_${ui/.ui/.h}
rm ${ui/.ui/.pro}
