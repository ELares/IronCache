#!/bin/sh
# #630 verify: every ACKED (key value) must still read back post-roll (zero acked-write loss).
set -u
lost=0; checked=0
while read -r k v; do
  checked=$((checked+1))
  got=$(redis-cli -c -h ironcache-1 -p 6379 -t 5 GET "$k" 2>/dev/null)
  if [ "$got" != "$v" ]; then lost=$((lost+1)); [ "$lost" -le 5 ] && echo "  LOST $k: want=$v got=$got"; fi
done < /out/acked.txt
echo "verify: checked=$checked lost=$lost"
