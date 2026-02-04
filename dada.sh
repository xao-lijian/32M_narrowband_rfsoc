dada_db -d -k a000 2>/dev/null || true
dada_db -k a000 -n 32 -b 268435456
dada_dbmonitor -k a000

