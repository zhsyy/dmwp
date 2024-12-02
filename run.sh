#!/bin/bash

repeat_time=2

echo "test qc"
for qc in {0..7}
do
    for ((i=1; i<=$repeat_time; i++))
    do
        echo "type dmwp-no-shuffle np = 5 hop = 2 thr = 1 qc = q_$qc window = 7 slidesize = 5 bucketsize = 100 第 $i 次循环"
        mpjrun.sh -Xmx230g -Xms20g -dev niodev -np 5 -jar dmwp-no-shuffle.jar -fp /root/dmwp/dataset/ -f SO-50w -alpha 1 -tc 10 -ws 7 -ss 1 -ms 300000 -hop 2 -thr 5 -q q_$qc
    done
done

echo "test np"
for np in {2..9}
do
    for ((i=1; i<=$repeat_time; i++))
    do
        echo "type dmwp-no-shuffle np = $np hop = 2 thr = 1 qc = q_0 window = 7 slidesize = 5 bucketsize = 100 第 $i 次循环"
        mpjrun.sh -Xmx230g -Xms20g -dev niodev -np $np -jar dmwp-no-shuffle.jar -fp /root/dmwp/dataset/ -f SO-50w -alpha 1 -tc 10 -ws 7 -ss 1 -ms 300000 -hop 2 -thr 5 -q q_0
    done
done
