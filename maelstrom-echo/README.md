## README

### Build and install program
```shell
go install
```

### Program to run
```shell
maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### Debugging Maelstrom
```shell
maelstrom serve
```
Results will be at [here](http://localhost:8080)