#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source ${SCRIPT_DIR}/common_settings.sh
source ${SCRIPT_DIR}/common/functions.sh

operation=$1

log "Script started at $(hostname), master replica status: ${MASTER_REPLICA}"

## TODO: add arg processing (to adjust vars in common_settings w/o editing files)

if [ "$operation" = "create_local_backup" ]; then
    create_local_backup
elif [ "$operation" = "reset_node" ]; then
    reset_node
elif [ "$operation" = "show_status" ]; then
    show_status
elif [ "$operation" = "recover_non_replicated" ]; then
    recover_schema_reattach_non_replicated_tables
elif [ "$operation" = "refill_replicated_tables" ]; then
    refill_replicated_tables
elif [ "$operation" = "recreate_kafka_tables" ]; then
    recreate_kafka_tables
else
    log "You need to pass operation as a script argument!"
    log "Possible operations:"
    log " * create_local_backup"
    log " * reset_node"
    log " * show_status"
    log " * recover_non_replicated"
    log " * refill_replicated_tables"
    log " * recreate_kafka_tables"
    log "See readme & source code for details."
    exit 1
fi

log "Finished!"
