END=10
## print date five times ##
x=$END 
while [ $x -gt 0 ]; 
do 
    echo "start test part2" >> output.txt;
    ./raft_test part2 >> output.txt;
    echo "start test part1" >> output.txt;
    ./raft_test part1 >> output.txt;
    x=$(($x-1));
done