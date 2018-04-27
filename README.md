# nomad_follower
Log forwarder for aggregating allocation logs from nomad worker agents.

## Running the application 
Run the application on each worker in a nomad cluster. nomad_follower will follow all allocations on the worker and tail the allocation logs to the log file. 
```docker pull adragoset/nomad_follower:latest```
```docker run -v log_folder:/log -e LOG_FILE="/logs/nomad-forwarder.log" adragoset/nomad_follower:latest```

nomad_follower will stop following completed allocations and will start following new allocations as they become available. nomad_follower can deployed with nomad in a task group along with a log collector. The aggregate log file can then be shared with the log collector by storing the aggregate log in the shared allocation folder. 

Using nomad_follower prevents the cluster operator from having to run a log collector in every task group for every task on a worker while still allowing nomad to handle the logs for each allocation. 
