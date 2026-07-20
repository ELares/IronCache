#!/bin/sh
# #630 load: WRITE unique values via a cluster-following client (redis-cli -c follows MOVED across
# the failover) and record every ACKED (key value); any SET failure is an outage tick. Runs inside
# a redis container on the compose network so ironcache-N DNS + MOVED redirects resolve.
set -u
OUT=/out; : > "$OUT/acked.txt"; : > "$OUT/errors.txt"
i=0; end=$(( $(date +%s) + ${DURATION:-120} ))
while [ "$(date +%s)" -lt "$end" ]; do
  k="smoke:$i"; v="val-$i"
  if [ "$(redis-cli -c -h ironcache-1 -p 6379 -t 5 SET "$k" "$v" 2>/dev/null)" = "OK" ]; then
    echo "$k $v" >> "$OUT/acked.txt"
  else
    echo "SET $k FAILED @$(date +%s)" >> "$OUT/errors.txt"
  fi
  i=$((i+1))
done
echo "writer: acked=$(wc -l < "$OUT/acked.txt") outages=$(wc -l < "$OUT/errors.txt")"
