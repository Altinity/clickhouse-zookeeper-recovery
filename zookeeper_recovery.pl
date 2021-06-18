#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper; # TODO: not a part of core modules on older perl
use POSIX();
use Carp;
$| = 1; # disable output buffering


##### params: #############

my $CLICKHOUSE_CLIENT = 'clickhouse-client';

# leave $CLUSTER_NAME to run on a single node (also check RECOVER_SCHEMA_ONLY)
# or run with cluster name and it should do everything correct on the whole cluster.
# the safe & handy way is to create a subcluster for every shard and run that tool shard by shard
my $CLUSTER_NAME = '';

# if set the data will not be recovered (can make sense with empty CLUSTER NAME will be synced from the other replica).
my $RECOVER_SCHEMA_ONLY = 0; 

# just output the commands which should be executed.
my $DRY_RUN = 0;

###########################


sub printlog {
  my $log_line = shift;  
  print (POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime time), " ", sprintf($log_line, @_), "\n");
}

sub escape_shell_arg {
   my ($arg) = @_;
   $arg =~ s/'/'\\''/g;
   $arg =~ s/^''//; $arg =~ s/''$//;
   return "'$arg'";
}

sub escape_non_ascii_for_sql {
    my ($arg) = @_;
    $arg =~ s/([^A-Za-z0-9_])/sprintf("\\x%02X", ord($1))/seg;
    return $arg;
}

sub escape_sql_arg {
    my ($arg) = @_;
    return q{'} . escape_non_ascii_for_sql($arg) . q{'};
}

# clickhouse perfectly accepts \xFF sequences in the identifiers with backticks,
sub full_table_name {
   my ($database, $table) = @_;
   return join '.', map {  q{`} . escape_non_ascii_for_sql($_) . q{`} } ($database, $table);
}

# TabSeparated: The following escape sequences are used for output: \b, \f, \r, \n, \t, \0, \', \\.
my %mapping = (
    "\\b" => "\b",  "\\f" => "\f",  "\\r" => "\r", "\\n" => "\n",  "\\t" => "\t",  "\\0" => "\0",
    "\\'" => "\'",  "\\\\" => "\\", "\\"  => "\\"
);

# return array of array
# tuples / maps / arrays - are not parsed
sub parse_tsv
{
    my ($tsv) = @_;
    my $res = [ map { [ map { s/(\\[bfrnt0'\\]|\\)/$mapping{$1}/seg; $_; } split "\t", $_, -1 ] } split "\n", $tsv, -1 ];
    if ( scalar(@{pop @$res}) != 0 )
    {
        confess("Newline at the end of TSV is missing!");
    }
    return $res;
}

# return array of hashes
sub parse_tsv_with_names
{
    my ($tsv) = @_;
    my $raa = parse_tsv($tsv);
    my $column_names = shift @$raa; # get header row
    my $res = [];
    foreach my $row (@$raa)
    {
        my %h;
        @h{@$column_names} = @$row;
        push @$res, \%h;
    }
    return $res;
}

sub run_clickhouse_query
{
    my $query = shift;
    my $extra_settings = shift || {};

    my @args = ("${CLICKHOUSE_CLIENT}");

    push @args, "--query=" . escape_shell_arg($query);

    while (my ($key, $value) = each (%$extra_settings)) {
        push @args, "--".$key."=" . escape_shell_arg($value);
    }

    my $cmd = join(' ', @args);
    my $output = `$cmd`;
    my $status = $?;
    return {
        status => $status,
        output => $output,
        cmd => $cmd,
    };
}

sub run_ddl_command
{
    my $query = shift;
    my $extra_settings = shift || {};

    my $retries = 1;

    while ($retries <= 5)
    {
        printlog('Executing%s: %s', $retries > 1 ?  "(attempt #$retries)" : '' , $query);

        if ($DRY_RUN)
        {
            printlog('Success! (DRY RUN)');
            return 1;
        }

        my $res = run_clickhouse_query($query, $extra_settings);
        if ($res->{status} == 0)
        {
            printlog('Success!');
            return 1;
        }

        printlog("Command failed: %s\n%s", $res->{cmd}, $res->{output});

        sleep($retries);
        $retries += 1;
    }

    confess('Too many failed attempts!');
}

# we print all planned commands, so in case something will break in the middle user can finish them manually.
sub run_ddl_command_sequence
{
    my $ddl_commands = shift;
    printlog("Trying to execute the following commands: \n %s;\n", join(";\n",  @$ddl_commands));
    run_ddl_command($_) foreach (@$ddl_commands);
}

sub get_clickhouse_query_result
{
    my $query = shift;
    my $extra_settings = shift || {};

    my $res = run_clickhouse_query($query, $extra_settings);
    # print Dumper $res;
    if ($res->{status} != 0)
    {
        confess("Command failed: ", $res->{cmd}, "\n", $res->{output});
    }
    my $output = $res->{output};
    chomp $output;
    return $output;
}

sub run_clickhouse_query2
{
    my $query = shift;
    my $extra_settings = shift || {};

    my $res = run_clickhouse_query($query, {%$extra_settings, format=>'TSVWithNames'});

    if ($res->{status} != 0)
    {
        confess("Can not connect: ", $res->{output});
    }
    # print Dumper $res;

    return parse_tsv_with_names($res->{output});

}

sub prompt_yn {
  my ($query) = @_;
  print "$query (Y/N) ";
  chomp(my $answer = <STDIN>);
  return lc($answer) eq 'y';
}

sub maybecluster {
    my $table_name = shift;
    return $CLUSTER_NAME ? 'clusterAllReplicas(' . $CLUSTER_NAME . ',' . $table_name . ')' : $table_name;
}

sub ddl_maybe_oncluster {
    return $CLUSTER_NAME ? 'ON CLUSTER ' . escape_sql_arg($CLUSTER_NAME) : '';
}

sub maybe_add_on_cluster_to_create_statement {
    my ($create_statement) = @_;
    my $on_cluster = ddl_maybe_oncluster();

    if ($on_cluster)
    {
        $create_statement =~ s/
            ^                                                # from begining
            (                                                # start capture group #1
                (?:CREATE|ATTACH)\s+TABLE\s+                 # CREATE OR ATTACH 
                (?:
                  (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)
                  \.
                )?                                             # optional name of the database (maybe quoted with backticks or doublequotes) followed by dot
                (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)\s+  # name of the table (maybe quoted with backticks or doublequotes)
                (?:UUID\s+'[0-9a-fA-F-]+'\s+)?               # optional uuid 
            )
            /$1 $on_cluster /isx;
    }

    return $create_statement;
}

sub rename_table_in_create_statement
{
    my ($create_table,$new_name) = @_; 
    print "0 $create_table\n";
    $create_table =~ s/
                ^                                            # from begining   
                (                                            # start capture group #1
                CREATE
                \s+TABLE\s+
                )
                (?:
                  (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)
                  \.
                )?                                             # optional name of the database (maybe quoted with backticks or doublequotes) followed by dot
                (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)\s+  # name of the table (maybe quoted with backticks or doublequotes)
                /$1$new_name /sxi;
    print "1 $create_table\n";
    return $create_table;

}


sub attach_as_non_replicated
{   
    my ($original_create_table) = @_; 
    print "2 $original_create_table\n";
    my $modified_attach_table = maybe_add_on_cluster_to_create_statement($original_create_table);
    print "3 $modified_attach_table\n";
    $modified_attach_table =~ s/
                ^                                            # from begining   
                CREATE
                (                                            # start capture group #1
                \s+TABLE\s+
                (?:
                  (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)
                  \.
                )?                                             # optional name of the database (maybe quoted with backticks or doublequotes) followed by dot
                (?:`(?:\\.|[^`])+`|"(?:\\.|[^"])+"|[a-zA-Z0-9_]+)\s+  # name of the table (maybe quoted with backticks or doublequotes)
                )                                            # end capture group #1
                (?:UUID\s+'[0-9a-fA-F-]+'\s+)?               # optional uuid 
                (.*)                                         # capture group #2
                ( \)\s+ENGINE\s*=\s* )                       # capture group #3
                   Replicated
                ([a-zA-Z]*MergeTree\()                       # capture group #4
                (?:\s*'(?:\\.|[^'])+'\s*,\s*'(?:\\.|[^'])+')                # params of Replicated
                 
                ([^\)]*\))                                   # capture group #5 - all other params + closing bracket.
                /ATTACH$1$2$3$4$5/sxi;
    print "4 $modified_attach_table\n";
    return $modified_attach_table;
}



sub print_general_info {
    my $res = run_clickhouse_query("SELECT 1");

    # check the conn is ok
    if ($res->{status} != 0 or $res->{output} != "1\n")
    {
        confess("Can not connect: ", $res->{output});
    }

    printlog( "Clickhouse:\n%s\n", 
         get_clickhouse_query_result(
            "SELECT
                hostName(),
                'ClickHouse ' || version() as v,
                uptime(),
                toString( now() - uptime() ) as start_time
            FROM ".maybecluster('system.one')."
            ORDER BY hostName()",
            {format => 'PrettyCompactMonoBlock'}
        )
    );

    printlog("Defined macros:\n%s\n", 
        get_clickhouse_query_result("
            SELECT
                hostName(),
                *
            FROM " . maybecluster('system.macros') . "
            ORDER BY hostName(), macro",
            {format => 'PrettyCompactMonoBlock'}
        )
    );

    printlog("Defined clusters:\n%s\n", 
        get_clickhouse_query_result("
            SELECT
                hostName(),
                *
            FROM " . maybecluster('system.clusters') . "
            WHERE cluster not like 'test\\\\_%'
            ORDER BY hostName(), cluster, shard_num, replica_num",
            {format => 'PrettyCompactMonoBlock'}
        )
    );

    printlog("Zookeeper:\n%s\n%s\n", 
        get_clickhouse_query_result("
            SELECT
                hostName(),
                *
            FROM " . maybecluster('system.zookeeper') . "
            WHERE path = '/'
            ORDER BY hostName(), name",
            {format => 'PrettyCompactMonoBlock'}
        ),
        get_clickhouse_query_result("
            SELECT
                hostName(),
                *
            FROM " . maybecluster('system.zookeeper') . "
            WHERE path = '/clickhouse'
            ORDER BY hostName(), name",
            {format => 'PrettyCompactMonoBlock'}
        )
    );
}

my $uuid_supported_cached_result = undef;

sub is_uuid_supported
{
    if (!defined($uuid_supported_cached_result))
    {
        $uuid_supported_cached_result = get_clickhouse_query_result("
        SELECT
            count() > 0
        FROM " . maybecluster('system.settings') . "
        WHERE name='show_table_uuid_in_table_create_query_if_not_nil'");

        printlog( 'show_table_uuid_in_table_create_query_if_not_nil supported: %d', $uuid_supported_cached_result );

    }
    return $uuid_supported_cached_result;
}

sub find_tables_with_zookeeper_data_missing
{
    printlog( 'Detecting tables with zookeeper missing...' );
    return run_clickhouse_query2("
        WITH
            is_readonly and not is_session_expired and zookeeper_exception like '%No node%' as zookeeper_data_missing
        SELECT
            database,
            table,
            uniqExact(zookeeper_path) as nr_of_zookeeper_paths,
            arrayStringConcat( groupArray((hostName() || ': ' || zookeeper_exception)), '\n') as zookeeper_exeptions,
            arrayStringConcat( groupArrayIf(hostName(),zookeeper_data_missing), ',') as hosts_with_zookeeper_data_missing,
            arrayStringConcat( groupArrayIf(hostName(),not zookeeper_data_missing), ',') as hosts_with_zookeeper_data
        FROM " . maybecluster('system.replicas') . "
        GROUP BY
            database,
            table
        HAVING countIf(zookeeper_data_missing) > 0
        ORDER BY
            database,
            table
    ");
}

sub get_table_info
{
    my ($database_name, $table_name) = @_;
    my $uuid_supported = is_uuid_supported();

    return run_clickhouse_query2(
        sprintf(
            'SELECT
                hostName(),
                *
            FROM %s
            WHERE database=%s AND name=%s
            ORDER BY hostName()',
            maybecluster('system.tables'),
            escape_sql_arg($database_name),
            escape_sql_arg($table_name)
        ),
        { $uuid_supported ? (show_table_uuid_in_table_create_query_if_not_nil => 1) : () }
    );
}

# table will be renamed to temporary name, recreated in place, and all partitions reattached back
sub recover_table_zookeeper_data 
{
    my ($table_name, $database_name, $temporary_db_name) = @_;

    my $full_table_name = full_table_name($database_name, $table_name);

    my $target_table_name = $RECOVER_SCHEMA_ONLY ? "${database_name}.${table_name}_origdata" : "${database_name}.${table_name}";
    my $full_tmp_table_name = full_table_name($temporary_db_name, $target_table_name);
    
    printlog( 'Processing %s, using %s as temporary table', $full_table_name, $full_tmp_table_name);

    my $original_table_rows_count = get_clickhouse_query_result(sprintf('SELECT count() FROM %s',$full_table_name));

    my $table_info = get_table_info($database_name, $table_name);

    if (scalar(@$table_info) == 0) {
        confess('Empty result of system.tables query');
    }

    my $target_table_info = get_table_info($temporary_db_name,$target_table_name);

    if (scalar(@$target_table_info) > 0)
    {
        print Dumper $target_table_info;
        confess("Temporary table $full_tmp_table_name already exists! Do cleanup manually to continue.");
    }

    # small consistency check - ensure the schema is the same for different nodes 
    my $original_create_table = $table_info->[0]{create_table_query};

    if ( scalar(@$table_info) > 1 )
    { 
        for my $v (@$table_info)
        {
            if ( $v->{create_table_query} ne $original_create_table) {
                printlog( '%s statement : %s', $v->{'hostName()'}, $v->{create_table_query});
                printlog( '%s statement : %s', $table_info->[0]{'hostName()'}, $table_info->[0]{create_table_query});
                confess('Table schema is inconsistant across the cluster nodes!');
            }
        }
    }

    my $parts_info = run_clickhouse_query2(
        sprintf(
            'SELECT
                partition_id,
                uniqExact(name) as parts_count
            FROM %s
            WHERE 
                active
                AND database=%s AND table=%s
            GROUP BY partition_id
            ORDER BY partition_id',
            maybecluster('system.parts'),
            escape_sql_arg($database_name),
            escape_sql_arg($table_name)
        )
    );

    if (scalar(@$parts_info) == 0)
    {
        printlog('Empty result of system.parts query: table is empty, will just recreate it.');

        run_ddl_command_sequence(
            [
                sprintf('DROP TABLE IF EXISTS %s %s NO DELAY', $full_table_name, ddl_maybe_oncluster()),
                maybe_add_on_cluster_to_create_statement($original_create_table),
            ]
        );

        return;
    }

    my $max_part_per_partition = 0;
    my $overall_number_of_parts = 0;

    for my $p (@$parts_info)
    {
        if ($p->{parts_count} > $max_part_per_partition)
        {
            $max_part_per_partition = $p->{parts_count};
            $overall_number_of_parts += $p->{parts_count};
        }
    }

    # TODO: do we care of replicated_deduplication_window here?
    printlog("max_part_per_partition: %d, overall_number_of_parts: %d", $max_part_per_partition, $overall_number_of_parts);

    
    my @command_sequence = ();

    # inside Atomic database that doesn't work: 
    # DB::Exception: Mapping for table with UUID=ccbe67e0-eb08-4897-80f1-404c3b488810 already exists. It happened due to UUID collision, most likely because some not random UUIDs were manually specified in CREATE queries. (version 21.7.1.7029 (official build))

    # so we do that reattach only in after moving the table to ordinary to 'drop' atomic nature of it. 

    # push @command_sequence, sprintf('DETACH TABLE IF EXISTS %s %s NO DELAY', $full_table_name, ddl_maybe_oncluster());
    # push @command_sequence, attach_as_non_replicated($original_create_table);

    # SYSTEM STOP MERGES don't work cluster-wide
    # the safest way to use that is to create a subcluster for every shard and do it shard by shard
    push @command_sequence, sprintf('SYSTEM STOP MERGES %s', $full_table_name);

    # direct rename or r/o table was not working before 20.5, see https://github.com/ClickHouse/ClickHouse/pull/11652/
    push @command_sequence, sprintf('RENAME TABLE %s TO %s %s', $full_table_name, $full_tmp_table_name, ddl_maybe_oncluster());
    
    push @command_sequence, sprintf('DETACH TABLE IF EXISTS %s %s NO DELAY', $full_tmp_table_name, ddl_maybe_oncluster());
    push @command_sequence, attach_as_non_replicated(rename_table_in_create_statement($original_create_table, $full_tmp_table_name));

    push @command_sequence, sprintf('SYSTEM STOP MERGES %s', $full_tmp_table_name);

    push @command_sequence, maybe_add_on_cluster_to_create_statement($original_create_table);
    push @command_sequence, sprintf('SYSTEM STOP MERGES %s', $full_table_name);

    if (!$RECOVER_SCHEMA_ONLY)
    {
        for my $p (@$parts_info)
        {
            push @command_sequence, sprintf('ALTER TABLE %s %s REPLACE PARTITION ID %s FROM %s',
                                            $full_table_name,
                                            ddl_maybe_oncluster(),
                                            escape_sql_arg($p->{partition_id}),
                                            $full_tmp_table_name);
        }
    }

    run_ddl_command_sequence(\@command_sequence);

    my $new_table_row_count = get_clickhouse_query_result(sprintf('SELECT count() FROM %s',$full_table_name));

    printlog('original_table_rows_count: %d, new_table_row_count: %d', $original_table_rows_count, $new_table_row_count);

    run_ddl_command(sprintf('SYSTEM START MERGES %s', $full_table_name));
}


printlog('Started %s [pid:%d]:', $0, $$);

print_general_info();

my $readonly_tables = find_tables_with_zookeeper_data_missing();

printlog( '%d tables with zookeeper_data_missing found.', scalar(@$readonly_tables));

if (scalar(@$readonly_tables) == 0) {
    printlog( 'Nothing to to!' );
    exit;
}

printlog( 'WARNING: Please stop the insertion to all the tables, detach all the Kafka / RabbitMQ / Buffer / Distributed tables!');
prompt_yn('Continue?') || exit(1);

my $temporary_db_name = '_tmp_zk_rcvry';
run_ddl_command(sprintf('CREATE DATABASE IF NOT EXISTS %s %s engine=Ordinary', $temporary_db_name, ddl_maybe_oncluster()));

foreach my $table (@$readonly_tables) {
    next if $table->{'database'} eq $temporary_db_name;

    recover_table_zookeeper_data($table->{'table'}, $table->{'database'}, $temporary_db_name);
}

printlog('Done! Cross check everything and remove %s database', $temporary_db_name);
