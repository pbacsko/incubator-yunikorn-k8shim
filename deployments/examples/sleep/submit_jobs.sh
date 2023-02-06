#!/bin/bash

curl http://localhost:9080/debug/pprof/heap?debug=1 > yk_memstats
echo "*************************" >> yk_memstats

for i in {1..3000}
do
  tmp=batch-sleep-job-template-$i.yaml
  sed "s/{{counter}}/$i/g" batch-sleep-job-template.yaml > $tmp
  kubectl apply -f $tmp
  sleep 1
  if [ $((i%200)) == "0" ]; then
    echo "Iteration: $i" >> yk_memstats
    curl http://localhost:9080/debug/pprof/heap?debug=1 >> yk_memstats
    echo "*************************" >> yk_memstats
  fi
  rm $tmp
done

echo "Sleep 30 mins..."
sleep 1800
echo "*************************" >> yk_memstats
echo "30 mins later" >> yk_memstats
curl http://localhost:9080/debug/pprof/heap?debug=1 >> yk_memstats
