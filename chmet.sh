met=$1
sed -i "s/METHOD = \".*\"/METHOD = \"$met\"/g" proxy/server.py
