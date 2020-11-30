# some settings
set -e # stop on error
#set -x # print the commands we execute

### ADJUST THOSE:

CLICKHOUSE_WORKING_FOLDER=/var/lib/clickhouse

# should be same disk as CLICKHOUSE_WORKING_FOLDER! (otherwise we can't use hardlinks)
CLICKHOUSE_TOOLSET_FOLDER=/var/lib/clickhouse/clickhouse-toolset

BACKUP_FOLDER="${CLICKHOUSE_TOOLSET_FOLDER}/backup2020-11-10"

# if you need some adjustments - like username/password/port/listened host or some parameter - adjust it here.
CLICKHOUSE_CLIENT='clickhouse-client --host=127.0.0.1 --max_query_size=10000000'

CLICKHOUSE_EXTRACT_FROM_CONFIG='clickhouse-extract-from-config --config-file /etc/clickhouse-server/config.xml'

# for replicated tables we should use data only
# from single replica (others will replicate)
# otherwise we will have replicated data

# if last character of the hostname is 1 we are on the master replica.
HOSTNAME_SHORT=$(hostname -s)
MASTER_REPLICA=$( [ "${HOSTNAME_SHORT: -1}" == "1" ] && echo 'true' || echo 'false' )

### TODO: expose settings above via command-line args

### those normally should not be changed

METADATA_FOLDER="${CLICKHOUSE_WORKING_FOLDER}/metadata"
DATA_FOLDER="${CLICKHOUSE_WORKING_FOLDER}/data"

BACKUP_METADATA_FOLDER="${BACKUP_FOLDER}/metadata"
BACKUP_DATA_FOLDER="${BACKUP_FOLDER}/data"

# we do mv instead of rm -rf (just in case), that folder is used as trashbin
TRASHBIN_FOLDER="${CLICKHOUSE_TOOLSET_FOLDER}/trashbin_$(date +%Y%m%d_%H%M%S)"

# we will put some tmp files there
TMP_FOLDER="${CLICKHOUSE_TOOLSET_FOLDER}/tmp"
