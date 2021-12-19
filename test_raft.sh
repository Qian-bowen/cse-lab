END=10
x=$END 
echo "" > output.txt
while [ $x -gt 0 ]; 
do 
    echo "test";
    echo "start test part3" >> output.txt;
    ./raft_test part2 >> output.txt;
    # echo "start test part2" >> output.txt;
    # ./raft_test part2 >> output.txt;
    # echo "start test part1" >> output.txt;
    # ./raft_test part1 >> output.txt;
    x=$(($x-1));
    sleep 1;
done
echo "done"
