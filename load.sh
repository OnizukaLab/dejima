# parameters
# ----------
# num of peers
N=$1
# num of records for each peer's insert
n=$2
# start id
start_id=$3

for i in `seq 1 $N`
do
		curl -s "localhost:$((i+8000))/load?start_id=${start_id}&record_num=${n}" >/dev/null
		start_id=$((start_id+n))
done

echo "finished"
