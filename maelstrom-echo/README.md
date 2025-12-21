## README

Please take a look at https://fly.io/dist-sys/1/ for details.

### Build and install the program
```shell
go install
```

### Program to run
```shell
maelstrom test -w echo --bin ~/go/bin/maelstrom-echo --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### Maelstrom UI
```shell
maelstrom serve
```
Results will be at [here](http://localhost:8080)