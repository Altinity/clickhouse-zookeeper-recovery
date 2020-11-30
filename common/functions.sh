perl -v > /dev/null || (echo 'no perl installed!'; exit 1)

mkdir -p $TMP_FOLDER || (echo "can not create tmp folder: $TMP_FOLDER (forget sudo?)"; exit 1)

log() {
   echo "$(date "+%Y-%m-%d %H:%M:%S.%N") $1"
}

log_and_run_command() {
   log "     Executing: '$*'"
   "$@"
}

copy_folder_by_hardlinks() {
   local source="$1"
   local target="$2"
   log_and_run_command cp -rla "$source" "$target"
}

execute_with_retries() {
   local i
   for i in {1..20}; do
        set +e
        "$@";
        local result=$?
        set -e
        if [ "$result" == 0 ]; then
            return 0;
        else
            log "    ! Error on try #${i}, will retry in 3 sec"
            sleep 3;
        fi
   done
   log "    ! Too many attempts!"
   return 1;

}

clickhouse_client_call() {
   local db_fs_name="$1"
   local add=''
   if [ -n "$db_fs_name" ]; then
      add=" --database=\$'$( urldecode "$db_fs_name" )'"
   fi
   eval ${CLICKHOUSE_CLIENT}${add}
}

run_clickhouse_query() {
   local db_fs_name="$1"
   local query="$2"
   echo "$query" | clickhouse_client_call "$db_fs_name"
}

run_clickhouse_query_with_retries() {
    execute_with_retries run_clickhouse_query "$@"
}

execute_metadata_file() {
   local db_fs_name="$1"
   local metadata_file="$2"
   log "    Executing $metadata_file: $(head -n 1 $metadata_file)"
   cat "$metadata_file" | clickhouse_client_call "$db_fs_name"
}

execute_metadata_file_with_retries() {
    execute_with_retries execute_metadata_file "$@"
    # we don't want to merge anything for now,
    # STOP MERGES only stop merges for existsing tables
    # so we repeat it after every table creation
    run_clickhouse_query_with_retries "" "SYSTEM STOP MERGES"
}

metadata_file_change_attach_to_create() {
   local metadata_file="$1"
   local new_metadata_file="$2"
   log "    Changing $metadata_file to CREATE"
   perl -0777 -npe 's/^ATTACH/CREATE/;' "$metadata_file" > $new_metadata_file
}

metadata_file_change_to_non_replicated_with_prefix () {
   local metadata_file="$1"
   local new_metadata_file="$2"
   log "    Changing $metadata_file to non-replacted with .recovered_non_repl. prefix"
   # https://regex101.com/r/pscML2/2
   # https://regex101.com/r/X4uwt5/2
   # + bash escaping
   # TODO: support for default_replica_path ?
   perl -0777 -npe $'s/ENGINE\\s*=\\s*Replicated((?:[A-Z][a-z]+)?MergeTree\\()(\'((?:\\\\\'|.)*?)\'),\\s*(\'((?:\\\\\'|.)*?)\')(?:,\\s*)?/ENGINE = $1/; s/^ATTACH\\s+TABLE\\s+(?:`((?:\\\\`|.)+?)`|(\\S+))/ATTACH TABLE `.recovered_non_repl.$1$2`/;' "$metadata_file"  > $new_metadata_file
}

create_object_from_metadata_file_with_retries() {
    local db_fs_name="$1"
    local metadata_file="$2"
    local new_metadata_file="$(mktemp --tmpdir="${TMP_FOLDER}" change_attach_2_create.XXXXXXX.sql)"
    metadata_file_change_attach_to_create "$metadata_file" "$new_metadata_file"

    execute_metadata_file_with_retries "$db_fs_name" "$new_metadata_file"
    rm $new_metadata_file
}

attach_object_as_non_replicated_with_retries() {
    local db_fs_name="$1"
    local metadata_file="$2"
    local new_metadata_file="$(mktemp --tmpdir="${TMP_FOLDER}" change_to_recovered_non_repl.XXXXXXX.sql)"
    metadata_file_change_to_non_replicated_with_prefix "$metadata_file" "$new_metadata_file"

    execute_metadata_file_with_retries "$db_fs_name" "$new_metadata_file"
    rm $new_metadata_file
}

# based on https://stackoverflow.com/a/37840948/1555175
# clickhouse perfectly accepts \xFF sequences in the identifiers with backticks,
# so can just directly map path path%20with%20special%20chars into DB object `path\0x20with0x20special0x20chars`
# it's much simpler than dealing with backslash escaping
urldecode() {
    : "${*//+/ }"
    echo "${_//%/\\x}"
    #echo -e "${_//%/\\x}"
}

# transofms database%201 table%201 => `database\x201`.`table\x201`
get_db_object_name() {
    local db_fs_name="$1"
    local table_fs_name="$2"
    echo "\`$( urldecode "$db_fs_name" )\`.\`$( urldecode ${table_fs_name})\`"
}

create_database() {
   local db_fs_name="$1"
   local db_metadata_file="$2"
   if [ "$db_fs_name" = 'default' ]; then
      log "  Database 'default' exists"
   else
      log "  Creating database: $( urldecode "$db_fs_name" )"
      create_object_from_metadata_file_with_retries "" "$db_metadata_file"
   fi
}

do_nothing() {
   true
}

copy_table_datadir_by_hardlinks()
{
   local db_fs_name="$1"
   local table_fs_name="$2"
   local new_table_fs_name="${3:-$table_fs_name}"
   if [ -d "${BACKUP_DATA_FOLDER}/${db_fs_name}/${table_fs_name}" ]; then
      log "    Copy data $( get_db_object_name "$db_fs_name" "${table_fs_name}") (by hardlinks):"
      copy_folder_by_hardlinks "${BACKUP_DATA_FOLDER}/${db_fs_name}/${table_fs_name}" "${DATA_FOLDER}/${db_fs_name}/${new_table_fs_name}"
   else
      log "    No datadir for $( get_db_object_name "$db_fs_name" "${table_fs_name}") in ${BACKUP_DATA_FOLDER}/${db_fs_name}/${table_fs_name}"
   fi
}

fill_replicated_table_by_reattaching_partitions() {
   local db_fs_name="$1"
   local source_table_fs_name="$2"
   local dest_table_fs_name="$3"

   local db_ch_name=$( urldecode "$db_fs_name" )
   local source_table_ch_name=$( urldecode "$source_table_fs_name" )
   local dest_table_ch_name=$( urldecode "$dest_table_fs_name" )

   local source_table_ch_full_name=$( get_db_object_name "$db_fs_name" "$source_table_fs_name" )
   local dest_table_ch_full_name=$( get_db_object_name "$db_fs_name" "$dest_table_fs_name" )

   log "    Stopping merges for the source table $source_table_ch_full_name."
   run_clickhouse_query_with_retries "$db_fs_name" "SYSTEM STOP MERGES $source_table_ch_full_name"

   local i
   for i in {1..100}; do
        if [ $( run_clickhouse_query "" "select progress from system.merges where database='$db_ch_name' and table='$source_table_ch_name' limit 1") ];
        then
           log "    There are merges running on $source_table_ch_name, waiting for 3 seconds"
           run_clickhouse_query_with_retries "" "SYSTEM STOP MERGES $source_table_ch_full_name"
           sleep 3
        else
           break
        fi
   done

   while read partitionid ; do
      log "     * Processing partition: $partitionid."
      run_clickhouse_query_with_retries "" "ALTER TABLE $dest_table_ch_full_name REPLACE PARTITION ID '$partitionid' FROM $source_table_ch_full_name";
   done < <( run_clickhouse_query "" "select partition_id from system.parts where active and database='$db_ch_name' and table='$source_table_ch_name' GROUP BY partition_id ORDER BY partition_id FORMAT TSV" )

   source_rows=$(run_clickhouse_query "" "select count() from $source_table_ch_full_name" )
   target_rows=$(run_clickhouse_query "" "select count() from $dest_table_ch_full_name" )

   log "    The number of rows in ${source_table_ch_full_name}:  ${source_rows}"
   log "    The number of rows in ${dest_table_ch_full_name}:  ${target_rows}"

    if [ "$source_rows" != "$target_rows" ]; then
        log "The number of rows in ${dest_table_ch_full_name} is different from the number of rows in ${dest_table_ch_full_name}"
        log "The migration is interrupted"
        exit 1
    fi
}


attach_local_tables_and_skip_kafka()
{
   local db_fs_name="$1"
   local table_fs_name="$2"
   local table_metadata_full_filename="$3"

   if grep -qiE "Engine\\s*=\\s*Replicated\\w*MergeTree\\(" "$table_metadata_full_filename"; then
      log "    ... Replicated, attaching as .recovered_non_repl.${table_fs_name}"
      copy_table_datadir_by_hardlinks "$db_fs_name" "$table_fs_name" "%2Erecovered_non_repl%2E${table_fs_name}"
      attach_object_as_non_replicated_with_retries "$db_fs_name" "$table_metadata_full_filename"

   elif grep -qiE "Engine\\s*=\\s*Kafka" "$table_metadata_full_filename"; then
      # TODO: skip also Rabbit
      log "    ... Kafka, skipping for now"
      # we don't want to start inserts immediately
   else
      log "    ... non Replicated, attaching as is."
      copy_table_datadir_by_hardlinks "$db_fs_name" "$table_fs_name"
      execute_metadata_file_with_retries "$db_fs_name" "$table_metadata_full_filename"
      # they can rely on each other but normally clickhouse allows to do ATTACH even
      # with non-satisfied dependancies
   fi
}

create_replicated_tables_and_reattach_parts() {
   local db_fs_name="$1"
   local table_fs_name="$2"
   local table_metadata_full_filename="$3"

   if grep -qiE "Engine\\s*=\\s*Replicated\\w*MergeTree\(" "$table_metadata_full_filename"; then

      # that will fail if table don't exists

      res=$( run_clickhouse_query "" "SHOW CREATE TABLE \`$(urldecode "$db_fs_name")\`.\`.recovered_non_repl.$(urldecode "$table_fs_name")\`" )

      if [ -z "$res" ]; then
         log "    Can not find recovered_non_repl for ${table_fs_name}. Did you run recover_non_replicated before?"
         exit 1;
      fi

      create_object_from_metadata_file_with_retries "${db_fs_name}" "$table_metadata_full_filename"

      if [ "$MASTER_REPLICA" = 'true' ]; then
         log "    Script is running on master replica, reattaching parts"
         fill_replicated_table_by_reattaching_partitions "$db_fs_name" ".recovered_non_repl.${table_fs_name}" "$table_fs_name"
      else
         log "    Non-master replica, will sync the data from the another one"
      fi

      # ensure the data were flushed before removing
      sync

      log "    ... Dropping .recovered_non_repl.${table_fs_name}."
      run_clickhouse_query_with_retries "" "DROP TABLE IF EXISTS \`$(urldecode "$db_fs_name")\`.\`.recovered_non_repl.$(urldecode "$table_fs_name")\`;"
   else
      log "    ... non Replicated, skipping."
   fi
}

create_kafka_tables()
{
   local db_fs_name="$1"
   local table_fs_name="$2"
   local table_metadata_full_filename="$3"

   if grep -qiE "Engine\\s*=\\s*Kafka" "$table_metadata_full_filename"; then
      log "    Recreating the Kafka table"
      create_object_from_metadata_file_with_retries "${db_fs_name}" "$table_metadata_full_filename"
   else
      log "    ... non Kafka, skipping."
   fi
}


## TODO support for Atomic (/store folder & symlinks)
## TODO support for several disks

iterate_databases_and_tables_in_metadata() {
   local on_new_database="$1"
   local on_new_table="$2"

   local db_metadata_full_filename

   shopt -s nullglob # avoid returning * on empty dir

   for db_metadata_full_filename in "${BACKUP_METADATA_FOLDER}"/*.sql; do
      local db_metadata_filename="${db_metadata_full_filename##*/}"

      # the name of db in filesystem (folders etc)
      local db_fs_name="${db_metadata_filename%.sql}"

      # the real name is urldecoded db_fs_name
      log "> Database $( urldecode "$db_fs_name" ) found in $db_metadata_full_filename"

      if [ "$db_fs_name" = 'system' ]; then
         log "  ... skipping system database."
         continue
      fi

      $on_new_database "$db_fs_name" "$db_metadata_full_filename"

      log "  Iterating tables metadata in ${BACKUP_METADATA_FOLDER}/${db_fs_name}"

      local table_metadata_full_filename
      for table_metadata_full_filename in "${BACKUP_METADATA_FOLDER}/${db_fs_name}"/*.sql; do
         local table_metadata_filename="${table_metadata_full_filename##*/}"

         # the name of filesystem in filesystem (folders etc)
         local table_fs_name="${table_metadata_filename%.sql}"

         log ">>> Table $( get_db_object_name "$db_fs_name" "${table_fs_name}") found in ${table_metadata_full_filename}"
         $on_new_table "$db_fs_name" "$table_fs_name" "$table_metadata_full_filename"
      done
   done
}

ensure_clickhouse_is_stopped() {
    log 'checking if clickhouse is active.'
    set +e
    $CLICKHOUSE_CLIENT --query="SELECT 1" > /dev/null 2>&1
    local result=$?
    set -e
    if [ "$result" == 0 ]; then
        log 'ClickHouse is running. We can not reset it while it is active. Shutdown clickhouse first to continue!..'
        exit 1
    fi
    log 'It seems clickhouse is not running'
}





create_local_backup() {
    # we create/recover this 'backup' using hardlinks
    # warning: it's safe only when clickhouse is stopped
    # warning: file & its hardlink copy will have the same attributes (don't chown / chmod it!).
    # warning: data is not always immutable in clickhouse files (engine=Log, so after recovery backup can be affected by the running queries).


    if [ -d $BACKUP_FOLDER ]; then
        log "backup exists as $BACKUP_FOLDER . Can not continue"
        exit 1
    fi

    ensure_clickhouse_is_stopped

    log 'Creating backup folder'
    log_and_run_command mkdir -p "$BACKUP_FOLDER"

    log "Copy (by hardlinks) data & metadata folders"

    # TODO: we can do a real copy instead of hardlink copy for certain engines, and for metadata files.
    copy_folder_by_hardlinks "$METADATA_FOLDER" "$BACKUP_METADATA_FOLDER"
    copy_folder_by_hardlinks "$DATA_FOLDER" "$BACKUP_DATA_FOLDER"

    log 'Backup finished'
    log 'Now you can reset clickhouse node (reset_node) and clean up zookeeper (if it is broken)'
}

reset_node() {
    ## that script will move data & metadata aside to be able to start clickhouse
    ## second script will do the actual recovery.

    if [ ! -d $BACKUP_FOLDER ]; then
        log "backup does not exists at $BACKUP_FOLDER"
        exit 1
    fi

    ensure_clickhouse_is_stopped

    log "Creating trash bin"
    log_and_run_command mkdir -p "$TRASHBIN_FOLDER"

    log "Moving data and metadata to trash bin"
    log_and_run_command mv "$METADATA_FOLDER" "$TRASHBIN_FOLDER"
    log_and_run_command mv "$DATA_FOLDER" "$TRASHBIN_FOLDER"

    log "Recreating data & metadata folders"
    log_and_run_command mkdir -p "$METADATA_FOLDER" "$DATA_FOLDER"
    log_and_run_command chown -R clickhouse:clickhouse "$METADATA_FOLDER" "$DATA_FOLDER"

    log "Move back the system database (we don't expect any replicated tables there)"

    ### we don't expect any replicated tables in system database,
    ### and we want to put it into the correct place in advance
    ### otherwise clickhouse will recreate them automatically when it will be started

    if [ -d $BACKUP_METADATA_FOLDER/system ]; then
        copy_folder_by_hardlinks "$BACKUP_METADATA_FOLDER/system" "$METADATA_FOLDER"
    fi

    if [ -d $BACKUP_DATA_FOLDER/system ]; then
        copy_folder_by_hardlinks "$BACKUP_DATA_FOLDER/system"     "$DATA_FOLDER"
    fi

    log 'Node reset finished. Now you can start it (it will be empty).'
}

show_status() {
    set +e

    ### Check if we are active
    log 'Check status:'
    $CLICKHOUSE_CLIENT --query="SELECT 'ClickHouse ' || version() || ' at ' || hostName() || ' is up and running. Start time: ' || toString( now() - uptime() )" --format=TSVRaw

    log 'Macros:'
    $CLICKHOUSE_CLIENT --query="SELECT * FROM system.macros" --format=PrettyCompactMonoBlock

    log 'Clusters:'
    $CLICKHOUSE_CLIENT --query="SELECT * FROM system.clusters WHERE cluster not like 'test\\\\_%' " --format=PrettyCompactMonoBlock

    log 'Zookeeper:'
    $CLICKHOUSE_CLIENT --query="SELECT * FROM system.zookeeper WHERE path='/'" --format=PrettyCompactMonoBlock
    $CLICKHOUSE_EXTRACT_FROM_CONFIG --key=zookeeper
}

recover_schema_reattach_non_replicated_tables() {
    log "==================================================================================="
    log "==================================================================================="
    log " Iterating databases metadata in ${BACKUP_METADATA_FOLDER}:"
    log " Create databases, recover simple tables by attach, Replicated as non-replicated, skip Kafka"
    iterate_databases_and_tables_in_metadata "create_database" "attach_local_tables_and_skip_kafka"
    sync
}


refill_replicated_tables() {
    log "==================================================================================="
    log "==================================================================================="
    log " Iterating databases metadata in ${BACKUP_METADATA_FOLDER}, recreate Replicated table and reattach parts"
    iterate_databases_and_tables_in_metadata "do_nothing" "create_replicated_tables_and_reattach_parts"
    sync
}

recreate_kafka_tables() {
    log "==================================================================================="
    log "==================================================================================="
    log " Enabling merges "
    run_clickhouse_query_with_retries "" "SYSTEM START MERGES"

    log "==================================================================================="
    log "==================================================================================="
    log " Iterating databases metadata in ${BACKUP_METADATA_FOLDER}, recreate Kafka tables"
    iterate_databases_and_tables_in_metadata "do_nothing" "create_kafka_tables"
    sync
}


##########
## It is not used currently.
## It's safer to rely on ClickHouse to understand which folders need to be attached - because beside the tmp parts
## it can also contain same data in merged and unmerged form (and when you ATTACH part by part it will end up witj duplicates)
## In contrast when we attach whole folder as plain (non Replicated) MergeTree ClickHouse can understand that situations.
# reattach_parts()
# {
#    local db_fs_name="$1"
#    local table_fs_name="$2"

#    log "    Copy parts of the table $( get_db_object_name "$db_fs_name" "${table_fs_name}") (by hardlinks) from ${BACKUP_DATA_FOLDER}/${db_fs_name}/${table_fs_name} to ${DATA_FOLDER}/${db_fs_name}/${table_fs_name}/detached"

#    IGNORE_PARTS="^(detached|broken.*|unexpected.*|ignored.*|noquorum.*|tmp_mut.*)$"
#    shopt -s nullglob # avoid returning * on empty dir

#    local part_path
#    for part_path in "${BACKUP_DATA_FOLDER}/${db_fs_name}/${table_fs_name}"/*/; do
#       local part_name="${part_path%"${part_path##*[!/]}"}"  # extglob-free multi-trailing-/ trim
#       part_name="${part_name##*/}"              # remove everything before the last /
#       if [[ $part_name =~ $IGNORE_PARTS ]];
#       then
#          log "     - $part_name ignored ($part_path)"
#          continue
#       fi
#       log "     * ${part_name} at $part_path"
#       copy_folder_by_hardlinks "$part_path" "${DATA_FOLDER}/${db_fs_name}/${table_fs_name}/detached"
#       run_clickhouse_query_with_retries "$db_fs_name" "ALTER TABLE $( get_db_object_name "$db_fs_name" "${table_fs_name}") ATTACH PART '${part_name}'"
#    done
# }

# # Not used: metadata filename is the url-encoded table name
# extract_object_name_from_metadata_content() {
#     local db_fs_name="$1"
#     local metadata_file="$2"
#     # https://regex101.com/r/jea9p9/1/
#     perl -0777 -npe $'s/^(?:ATTACH|CREATE)\\s+(?:OR\\s+REPLACE\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?(TEMPORARY\\s+)?(?:MATERIALIZED\\s+VIEW|VIEW|DICTIONARY|TABLE|DATABASE|LIVE\\s+VIEW)\\s+(?:`((?:\\\\`|.)+?)`|(\\S+)).*$/$2$3/' "$metadata_file"
# }
