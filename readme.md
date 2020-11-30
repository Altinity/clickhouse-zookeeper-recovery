# ClickHouse zookeeper recovery tool

[ClickHouse](https://clickhouse.tech/) uses [ZooKeeper](https://zookeeper.apache.org/) for replication and for coordinating distributed operations on a cluster. While no data is stored in zookeeper (only metadata, like list of parts and their checksums) the zookeeper and metadata there is required to ClickHouse to work.

So if for some reason you lost your zookeeper data or it's damaged / out of the sync, then your ClickHouse servers will not start (or will start in read-only mode).
To return it back to a healthy state you you need to recover zookeeper meta information from the existing state of ClickHouse tables.

Those script can help you to automate that process even for clusters / big number of tables.

In simple cases you can do it manually (attach the Replicated table as non-Replicated, create new Replicated table, move all partitions from old table to new one). 

## Before you start

1. Analyze what happened.

   Usually if you loose the zookeeper data it means you configured something wrong, or did some innaccurate operations manually which lead to to that situation. 
  
2. review your setup, and try not to loose your zookeeper data anymore (otherwise you will need to repeat that recovery process again really soon)
   * use [recommended settings](https://clickhouse.tech/docs/en/operations/tips/#zookeeper) for Zookeeper
   * use 3 nodes zookeeper ensemble
   * set up good monitoring for your zookeeper. 

3. Ensure the data can't be recovered in better way. 

## How to use it

You can follow the sequence below cluster-wide using some automation scripts (like ansible) or just in cluster-ssh.

All steps (except step 5) may be executed on different replicas at different times. So you can recover them one-after-one, or simultaneously.

1) adjust paths/parameters `common_settings.sh`. The parameters are not (yet) configurable via command-line.

2) We will do direct interventions in clickhouse working folder, so clickhouse should be offline.
   
   ```
   sudo systemctl stop clickhouse-server
   ```

3) Create a backup of the data (using hard links).

   ```
   sudo ./toolset.sh create_local_backup
   ```

4) if you have some dirty state in zookeeper - clean it up. Do a backup (if needed) and run `deleteall /clickhouse`  in `zkCli`.

5) Run:

   ```
   sudo ./toolset.sh reset_node
   ```
   
   That will move the data & metadata of all known tables away. So generally, that will reset the state
   of your server - all tables & databases will disappear. (they are safe inside backup).

6) Start clickhouse back:

   ```
   sudo systemctl start clickhouse-server
   ```

   At that point, it should be clean - only system tables will be in place. The rest is saved inside backup.

7) Check the settings related to replication. Examine if they are correct:

   ```
   sudo ./toolset.sh show_status
   ```

8) Run:

   ```
   sudo ./toolset.sh recover_non_replicated | tee recover_non_replicated_$(date +%Y%m%d_%H%M%S).log
   ```
   
   That will recover the schema and data from the backup created on p. 3. Replicated table will be recovered w/o replication with another name (with `.recovered_non_repl.` prefix). Merges will be stopped, and we skip Kafka tables to avoid stating of consuming.

9) At that point, you can review the state of your data on different replicas.

   If needed, you can adjust/decide - which of them will be used as a source for recovery.
   
   **WARNING:** Only a single replica should have `MASTER_REPLICA=1` (otherwise, you will get data duplicates), it will be used to resync all data.
   
   Adjust parameters `common_settings.sh` if needed.

10) Run
    ```
    sudo ./toolset.sh refill_replicated_tables | tee refill_replicated_tables_$(date +%Y%m%d_%H%M%S).log
    ```
    That will create Replicated table back again.
    * If `MASTER_REPLICA=1` it will additionally copy partitions from `.recovered_non_repl.` table.
    *  The replicas which have `MASTER_REPLICA=0` will just create the table(s) and will sync the data from other ('MASTER') replica.
         * You can monitor the progress in `system.replication_queue` and/or `system.replicas`.
         * That may use a lot of network bandwidth.
         * On replicas which have `MASTER_REPLICA=0` you can also see the doubled disk usage (we refetch data from 'MASTER' replica while keeping own copy in the backup folder created in p.3)

11) Now, all tables/replicas should be back online. And now we can enable merges (were disabled on p.8) and start Kafka consuming:
    ```
    sudo ./toolset.sh recreate_kafka_tables | tee recreate_kafka_tables_$(date +%Y%m%d_%H%M%S).log
    ```


In case of any failures during the recovery:
1) fix the problem
2) stop clickhouse: `sudo systemctl stop clickhouse-server`
3) restart the recovery sequence from the p.4.

The tool does not clean the backup and trashbin folders. You can clean it manually after a successful recovery.

## Notes

Provided 'as is', use it at your own risk.
* All actions are transparent, and the log is quite verbose.
* We don't take any responsibility for potential data damage caused by inaccurate user actions related to that toolset.
* We used those scripts to recover the zookeeper data for a cluster with 10 nodes (5 shards / 2 replicas) with hundreds (about 700)  of tables.
* During all procedures, we keep the backup (using hard-links).
* In simpler cases (single table), recovery can be done manually.

Limitations: 
* It is not possible currently to recover zookeeper without downtime. 
* Because of hard links, all the actions executed on the source file will also affect hard link copy and vice versa. In most cases, files in clickhouse are immutable, but for engine=Log family (which are typically not used widely), it can be the problem. If you start modifying the `engine=Log` table just after recovery, the backup copy (which is not a real copy, but a hardlink) will be affected by those changes.
* Checked on last versions of Linux only (ubuntu 20, centos 7). 
* It doesn't support database=Atomic (yet?)
* It doesn't support multidisk setups (yet?) / s3 disks.

In newer ClickHouse versions a special command to automate that process (also to avoid full resync) may be added.
