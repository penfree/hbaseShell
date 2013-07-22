hbaseShell
==========

Hbase shell functionality from python

Provide full funcionality and flexible interface to the following HBase shell commands:


<strong> Group name: general</strong><br>
Commands: status, table_help, version, whoami

<strong>Group name: ddl</strong><br>
  Commands: alter, alter_async, alter_status, create, describe, disable, disable_all, drop, drop_all, enable, enable_all, exists, get_table, is_disabled, is_enabled, list, show_filters

<strong>Group name: dml</strong><br>
  Commands: count, delete, deleteall, get, get_counter, incr, put, scan, truncate

<strong>Group name: replication</strong><br>
  Commands: add_peer, disable_peer, enable_peer, list_peers, remove_peer, start_replication, stop_replication

<strong>Group name: snapshot</strong><br>
  Commands: clone_snapshot, delete_snapshot, list_snapshots, restore_snapshot, snapshot

<strong>Group name: security</strong><br>
  Commands: grant, revoke, user_permission


HBase shell output is returned as a list of strings.

In addition provides proxy functionality - executeCmd enables calling to Hbase shell with any command and parameter.
