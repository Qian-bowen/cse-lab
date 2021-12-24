END=100
## print date five times ##
x=$END 
echo "" > output1.txt
while [ $x -gt 0 ]; 
do 
    echo "test";
    echo "start test part2" >> output1.txt;
    ./raft_test part2 backup >> output1.txt;
    # echo "start test part1" >> output.txt;
    # ./raft_test part1 >> output.txt;
    x=$(($x-1));
    sleep 1;
done
echo "done"
