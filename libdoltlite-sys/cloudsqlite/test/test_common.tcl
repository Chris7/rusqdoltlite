
#
# COMMANDS USED BY TEST SCRIPTS:
#
#   bcv_create_container ?-namebytes N? ?-blocksize N? CONTAINER
#   bcv_destroy_container CONTAINER
#   bcv_upload_database ?-container CONTAINER? LOCAL ?REMOTE?
#   bcv_download_database ?-container CONTAINER? LOCAL ?REMOTE?
#   bcv_copy_database ?-container CONTAINER? FROM TO
#   bcv_delete_database ?-container CONTAINER? DB
#   bcv_sqlite3 ?-uri? ?-container CONTAINER? ?-daemon DAEMON? CMD FILENAME
#

set dirname [file dirname [info script]]
if {[info exists bcv_default_config]==0} {
  source [file join $dirname config.tcl]
}

set D(container) bcvtest5
set D(container2) bcvtest4
set D(dir) testdir
set D(blocksize) 131072
set D(cachesize)  1G
set D(nwrite)     10
set D(deletetime) 2
set D(retrytime)  10
set D(gctime)     3600
set D(httptimeout) 600

set D(httplogtimeout) 3600
set D(httplognentry) -1

proc make_container_name {name} {
  global C
  if {[info exists C(postfix)]} {
    append name $C(postfix)
  }
  return $name
}
set D(container)  [make_container_name $D(container)]
set D(container2) [make_container_name $D(container2)]

# Extract the options passed as an argument from the array above and
# return them as a list. e.g.:
#
#    swoptions -module -user
# 
# returns
#
#    {-module $O(-module) -user $O(-user)}
#
# with the two variables substituted from the parent context.
#
proc swoptions {args} {
  upvar O O
  set ret [list]
  foreach a $args { lappend ret $a $O($a) }
  set ret
}


# Create and return an Azure SAS token for container $zCont. A read-only
# token if parameter $bReadonly is true, or read-write otherwise. If $nSecond
# is set to zero, then the returned token is valid for 24 hours. Otherwise,
# it is valid for $nSecond seconds.
#
# This command is used as a C(sas_cmd) callback for some Azure tests.
#
proc make_azure_sas {bReadonly zCont {nSecond 0}} {
  global C
  if {$nSecond==0} { 
    set nSecond 86400 
    if {[info exists ::azure_sas_cache($zCont.$bReadonly)]} {
      return $::azure_sas_cache($zCont.$bReadonly)
    }
  } 
  sqlite3 db_make_sas ""
  set date [db_make_sas one {
    SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+'||$nSecond||' seconds')
  }]
  db_make_sas close
  set cmd [list az storage container generate-sas]
  lappend cmd --account-key $C(accesskey)
  lappend cmd --account-name $C(user)
  lappend cmd --name $zCont
  lappend cmd --expiry $date
  if {$bReadonly} {
    lappend cmd --permissions lr
  } else {
    lappend cmd --permissions dlrwac
  }
  lappend cmd --output tsv

  set res [exec {*}$cmd]
  if {$nSecond==86400} {
    set ::azure_sas_cache($zCont.$bReadonly) $res
  }

  set res
}

proc make_azure_create_sas {} {
  global C
  sqlite3 db_make_sas ""
  set date [db_make_sas one {
    SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+86400 seconds')
  }]
  db_make_sas close

  set cmd [list az storage account generate-sas]
  lappend cmd --account-key $C(accesskey)
  lappend cmd --account-name $C(user)
  lappend cmd --expiry $date
  lappend cmd --permissions acdlpruw
  lappend cmd --resource-types sco
  lappend cmd --services b
  lappend cmd --output tsv

  set sas [exec {*}$cmd]
  return "?$sas"
}

proc make_google_sas {bReadonly zCont {nSecond 0}} {
  global C

  set     cmd [list oauth2l fetch --cache ""]
  lappend cmd --credentials $C(accesskey) 
  if { $bReadonly==0 } {
    lappend cmd devstorage.full_control
  } else {
    lappend cmd devstorage.read_only
  }
  exec {*}$cmd
}

proc make_sas {zCont {seconds 0}} {
  global C
  set sas ""
  if {[info exists C(sas_cmd)]} {
    set sas [$C(sas_cmd) 0 $zCont $seconds]
  }
  set sas
}

proc make_readonly_sas {zCont {seconds 0}} {
  global C
  set sas ""
  if {[info exists C(sas_cmd)]} {
    set sas [$C(sas_cmd) 1 $zCont $seconds]
  }
  set sas
}

proc make_uri_sas {zCont {seconds 0}} {
  set sas [make_sas $zCont $seconds]
  set map [list = %3D    & %26   % %25]
  string map $map $sas
}

swproc bcv_open "
  {-module $C(module)} 
  {-user $C(user)} 
  {-authentication {$C(auth)}} 
  {-create 0} 
  container {bReadonly 0}
" {
  global C

  if {$O(-authentication)==""} {
    if {$O(-create) && [info exists C(sas_cmd2)]} {
      set O(-authentication) [$C(sas_cmd2)]
    } elseif {[info exists C(sas_cmd)]} {
      set O(-authentication) [$C(sas_cmd) $bReadonly $container]
    }
  }

  sqlite3_bcv_open $O(-module) $O(-user) $O(-authentication) $container
}

# bcv_attach ?OPTIONS? CONTAINER
# 
# where OPTIONS are:
#
#   -daemon INTEGER
#   -auth   AUTHORIZATION
#   -alias  ALIAS
#   -module CLOUD-MODULE
#   -user   USER
#
swproc bcv_attach "
  -inline -noerr -poll -readonly -secure
  {-daemon 0} {-auth auto} {-alias {}}
  {-module {}} {-user {}}
  container
" {
  global D
  global C

  if {$O(-auth) == "auto"} {
    set O(-auth) [make_sas $container]
  }

  if {$O(-inline) || ([info exists C(inlinecmd)] && $C(inlinecmd))} {
    set cmd sqlite3_bcv_attach
    if {$O(-poll)} { lappend cmd -poll }
    if {$O(-readonly)} { lappend cmd -readonly }
    if {$O(-noerr)} { lappend cmd -noerr }
    if {$O(-secure)} { lappend cmd -secure }
    lappend cmd $D(dir).$O(-daemon)
    lappend cmd $O(-module) $O(-user) $container $O(-auth) $O(-alias)
    eval $cmd
  } else {
    set cmd "./blockcachevfsd attach"
    if {$O(-poll)}     { lappend cmd -poll }
    if {$O(-readonly)} { lappend cmd -readonly }
    if {$O(-secure)} { lappend cmd -secure }
    lappend cmd -module $O(-module) -user $O(-user) -auth $O(-auth) 
    lappend cmd -alias $O(-alias) $D(dir).$O(-daemon) $container 
    set rc [catch { exec {*}$cmd } msg]
    if {$rc} {
      set msg [regsub {^error: } $msg {}]
      error $msg
    }
  }
}

swproc bcv_detach {-noerr {-daemon 0} cname} {
  global D
  global C
  if {[info exists C(inlinecmd)] && $C(inlinecmd)} {
    set cmd sqlite3_bcv_detach
    if {$O(-noerr)} { lappend cmd -noerr }
    eval [concat $cmd [list $D(dir).$O(-daemon) $cname]]
  } else {
    set     cmd "./blockcachevfsd detach $D(dir).$O(-daemon) $cname"
    set rc [catch { exec {*}$cmd } msg]
    if {$rc} {
      set msg [regsub {^error: } $msg {}]
      error $msg
    }
  }
}

swproc make_cmdline_args "
  {-module $C(module)}
  {-user $C(user)}
  {-authentication {$C(auth)}}
  {-create 0} {zContainer {}}
" {
  global C
  set ret [swoptions -module -user]
  if {$O(-authentication)==""} {
    if {$C(use_sas)==0} {
      if {[info exists C(secret)]} { lappend ret -auth $C(secret) }
    } else {
      if {$O(-create) && [info exists C(sas_cmd2)]} {
        lappend ret -auth [$C(sas_cmd2)]
      } elseif {$zContainer!=""} {
        lappend ret -auth [$C(sas_cmd) 0 $zContainer]
      }
    }
  } else {
    lappend ret -authentication $O(-authentication)
  }
  return $ret
}

swproc bcv_upload_database "
  {-module $C(module)}
  {-user $C(user)}
  {-authentication {$C(auth)}}
  {-container $D(container)} local {remote {}}
" {
  global C
  set opt [swoptions -module -user -authentication]
  if {$C(inlinecmd)} {
    set bcv [bcv_open {*}$opt $O(-container)]
    if {$remote==""} { set remote $local }
    set rc [$bcv upload $local $remote]
    if {$rc!=0} { error "upload failed ($rc) - [$bcv errmsg]" }
    $bcv close
  } else {
    set cmd "./blockcachevfsd upload"
    lappend cmd {*}[make_cmdline_args {*}$opt $O(-container)]
    lappend cmd -container $O(-container)
    lappend cmd $local
    if {$remote!=""} { lappend cmd $remote }
    exec {*}$cmd
  }
}

swproc bcv_download_database "{-container $D(container)} remote {local {}}" {
  global C
  if {$C(inlinecmd)} {
    set bcv [bcv_open $O(-container)]
    if {$local==""} { set local $remote }
    set rc [$bcv download $remote $local]
    if {$rc!=0} { error "download failed ($rc) - [$bcv errmsg]" }
    $bcv close
  } else {
    set     cmd "./blockcachevfsd download"
    lappend cmd {*}[make_cmdline_args $O(-container)]
    lappend cmd -container $O(-container)
    lappend cmd $remote
    if {$local!=""} { lappend cmd $local }
    exec {*}$cmd
  }
}

swproc bcv_copy_database "{-container $D(container)} from to" {
  global C
  if {$C(inlinecmd)} {
    set bcv [bcv_open $O(-container)]
    set rc [$bcv copy $from $to]
    if {$rc!=0} { set msg "copy failed ($rc) - [$bcv errmsg]" }
    $bcv close
    if {$rc!=0} { error $msg }
  } else {
    set     cmd "./blockcachevfsd copy"
    lappend cmd {*}[make_cmdline_args $O(-container)]
    lappend cmd -container $O(-container)
    lappend cmd $from
    lappend cmd $to
    exec {*}$cmd
  }
}

swproc bcv_delete_database "{-container $D(container)} db" {
  global C
  if {$C(inlinecmd)} {
    set bcv [bcv_open $O(-container)]
    set rc [$bcv delete $db]
    if {$rc!=0} { error "delete failed ($rc) - [$bcv errmsg]" }
    $bcv close
  } else {
    set     cmd "./blockcachevfsd delete"
    lappend cmd {*}[make_cmdline_args $O(-container)]
    lappend cmd -container $O(-container)
    lappend cmd $db
    exec {*}$cmd
  }
}

# Invoke the [blockcachevfsd create] command to create a container 
# named $cname.
swproc bcv_create_container "
  {-module $C(module)}
  {-user $C(user)}
  {-authentication {$C(auth)}}
  {-namebytes $C(namebytes)} 
  {-blocksize $D(blocksize)}
  -testnokv
  cname
" {
  global D
  global C
  set opt [swoptions -module -user -authentication]
  if {$C(inlinecmd)} {
    set bcv [bcv_open -create 1 {*}$opt $cname]
    if {$O(-testnokv)} {
      $bcv config testnokv 1
    }
    set rc [$bcv create $O(-namebytes) $O(-blocksize)]
    if {$rc!=0} { error "create failed ($rc) - [$bcv errmsg]" }
    $bcv close
  } else {
    set cmd "./blockcachevfsd create"
    lappend cmd -blocksize $O(-blocksize)
    lappend cmd -namebytes $O(-namebytes)
    lappend cmd {*}[make_cmdline_args {*}$opt -create 1 $cname]
    lappend cmd $cname
    exec {*}$cmd
  }
}

proc bcv_destroy_container {cname} {
  global C
  if {$C(inlinecmd)} {
    set bcv [bcv_open $cname]
    set rc [$bcv destroy]
    if {$rc!=0} { error "destroy failed ($rc) - [$bcv errmsg]" }
    $bcv close
  } else {
    set cmd "./blockcachevfsd destroy"
    lappend cmd {*}[make_cmdline_args $cname]
    lappend cmd $cname
    exec {*}$cmd
  }
}

proc list_files [list [list container $D(container)]] {
  global D
  set    cmd "./blockcachevfsd files "
  lappend cmd {*}[make_cmdline_args $container]
  lappend cmd $container
  exec {*}$cmd
}

proc make_dir {id} {
  global D
  if {$id < 0} {
    set dir $D(dir)
  } else {
    set dir $D(dir).$id
  }
  return $dir
}

proc start_daemon_error {id args} {
  array set O [process_daemon_args {*}$args]

  global D
  global C

  set     cmd "./blockcachevfsd daemon "
  lappend cmd -autoexit -dir [make_dir $id] -retry 2
  lappend cmd {*}[make_cmdline_args]
  lappend cmd {*}[daemon_args]
  if {$O(-noattach)==0 && $C(use_sas)==0} {
    lappend cmd $D(container)
  }

  exec {*}$cmd
}

proc start_daemon {id args} {
  array set O [process_daemon_args {*}$args]

  global D
  global C

  if {[info exists C(secure)]} { set O(-noattach) 1 }

  set dir [make_dir $id]
  set fdvar "::D(fd.$id)"

  set vconfig ""
  #set vconfig "valgrind --show-leak-kinds=all --leak-check=full"
  #set vconfig "valgrind --tool=callgrind"
  if {$O(-valgrind)} {
    set vconfig "valgrind --leak-check=full"
  }

  set     cmd "|${vconfig} ./blockcachevfsd daemon "
  lappend cmd -readymessage
  lappend cmd -autoexit 
  lappend cmd -nrequest $O(-nrequest)

  if {$O(-debug)} {
    lappend cmd -delay 5
  }
  lappend cmd {*}[daemon_args]
  lappend cmd $dir

  set $fdvar [open "$cmd | tee log.$id.txt 2>@1"]
  fconfigure [set $fdvar] -blocking 0
  fileevent [set $fdvar] readable [list daemon_msg $id [set $fdvar] $fdvar]
  set ::global_daemon_started($id) 0
  set pid [lindex [pid $D(fd.$id)] 0]
  puts -nonewline "starting daemon $id (pid=$pid)..."

  if {$O(-debug)} {
    set cmd2 [list xterm -fa mono -e gdb ./blockcachevfsd $pid &]
    exec {*}$cmd2
    wait_ms 2000
  }

  vwait ::global_daemon_started($id)
  if {[set $fdvar]==""} {
    error "Daemon failed to start!"
  } else {
    puts "started!"
  }

  if {$O(-noattach)==0 && $C(use_sas)} {
    bcv_attach -daemon $id $D(container)
  }
  # wait_ms 10000
}

proc daemon_msg {id fd fdvar args} {
  if {[eof $fd]} {
    fconfigure $fd -blocking 1
    if { [catch {close $fd} msg] } {
      puts $msg
    }
    set $fdvar ""
  } else {
    set str [gets $fd]
    if {$str!=""} {
      if {$str=="ready"} {
        incr ::global_daemon_started($id)
      } else {
        puts "$id: $str"
      }
    }
  }
}

proc wait_daemon {id} {
  if {$::D(fd.$id)!=""} {
    vwait ::D(fd.$id)
  }
}

proc start_new_system {args} {
  global D

#   # catch { file delete -force bcvtest.db }
  catch { file delete -force test.db1 test.db2 test.db3 }
  sqlite3 db test.db1 ; db eval { PRAGMA user_version = 0 } ; db close
  # sqlite3 db test.db2 ; db eval { PRAGMA user_version = 0 } ; db close
  # sqlite3 db test.db3 ; db eval { PRAGMA user_version = 0 } ; db close

  # catch { destroy_container }
  set cargs [list]
  set dargs [list]

  for {set i 0} {$i < [llength $args]} {incr i} {
    set a [lindex $args $i]
    if {$a=="-namebytes" || $a=="-blocksize"} {
      lappend cargs $a
      incr i
      lappend cargs [lindex $args $i]
    } else {
      lappend dargs $a
    }
  }
  bcv_create_container $D(container) {*}$cargs

  bcv_upload_database test.db1
  bcv_upload_database test.db1 test.db2
  bcv_upload_database test.db1 test.db3

  start_new_daemon 0 {*}$dargs
}

swproc process_daemon_args "
  -debug -vtab -nodelete -valgrind -noattach -lazy -nononce
  {-nrequest 1}
  {-namebytes $C(namebytes)} 
  {-blocksize $D(blocksize)}
  {-log {}}
  {-cachesize $D(cachesize)}
  {-httptimeout $D(httptimeout)}
  {-httplogtimeout $D(httplogtimeout)}
  {-httplognentry $D(httplognentry)}
" {
  concat [array get O]
}

proc extract_args {lArg lBool} {
  upvar O O
  set ret [list]
  foreach b $lBool {
    if {$O($b)} { lappend ret $b }
  }
  foreach a $lArg {
    lappend ret $a $O($a)
  }
  set ret
}

proc daemon_args {} {
  upvar O O
  lappend a -log -cachesize -httptimeout -nrequest -httplogtimeout -httplognentry
  set b {-vtab -nodelete -lazy -nononce}
  extract_args $a $b
}

proc daemon_args_debug {} {
  upvar O O
  concat [daemon_args] [extract_args {} {-debug -valgrind -noattach}]
}

proc start_new_daemon {id args} {
  array set O [process_daemon_args {*}$args]
  set dir [make_dir $id]
  file delete -force $dir
  file mkdir $dir
  start_daemon $id {*}[daemon_args_debug]
}

proc wait_ms {ms {reason ""}} {
  set ::global_var 0
  after $ms { set ::global_var 1 }
  puts -nonewline "...waiting ${ms}ms"
  if {$reason != ""} {
    puts -nonewline " $reason"
  }
  puts "..."
  vwait ::global_var
}

proc make_filename {cont name {id -1}} {
  file join [make_dir $id] $cont $name
}
proc make_path {name id} {
  make_filename $::D(container) $name $id
}

proc do_test {tn script res} {
  global G
  set testname "$::testprefix-$tn..."
  if {[info exists ::testsuite]} {
    set testname "$::testsuite.$testname"
  }

  set t1 [clock milliseconds]
  puts -nonewline "$testname..."
  set err [catch {uplevel $script} got]
  puts -nonewline "[expr [clock milliseconds] - $t1]ms.."
  if {$err} {
    puts "FAILED"
    puts "error: $got"
    incr G(nFail)
    lappend G(lFail) $testname
  } elseif {$res==$got} {
    puts "ok"
    incr G(nPass)
  } else {
    puts "FAILED"
    puts "expected \"$res\" got \"$got\""
    incr G(nFail)
    lappend G(lFail) $testname
  }
}

proc execsql {sql {db db}} {
  uplevel [list $db eval $sql]
}

proc do_execsql_test {tn sql {res {}}} {
  uplevel [list do_test $tn [list db eval $sql] [list {*}$res]]
}
proc do_execsql2_test {tn sql {res {}}} {
  uplevel [list do_test $tn [list db2 eval $sql] [list {*}$res]]
}
proc do_execsql3_test {tn sql {res {}}} {
  uplevel [list do_test $tn [list db3 eval $sql] [list {*}$res]]
}

proc do_catch_test {tn tcl res} {
  set emsg [lindex $res 1]

  uplevel [list do_test $tn [subst -nocom {
    set rc [catch "$tcl" msg]
    if {[string match {$emsg} [set msg]]} {
      set msg {$emsg}
    }
    list [set rc] [set msg]
  }] $res]
}

proc do_catchsql_test {tn sql res} {
  uplevel [list do_catch_test $tn "db eval {$sql}" $res]
}

proc wait_on_user_version {db iVersion timeout} {
  puts "...waiting up to ${timeout}ms for $db to see user_version=${iVersion}..."
  set ::user_version_time 0
  set n [expr ($timeout+99) / 100]
  set t1 [clock milliseconds]
  for {set i 0} {$i<$n} {incr i} {
    set rc [catch {
        $db eval { PRAGMA user_version }
    } v]
    if {$rc} {
      wait_ms 10
      puts stderr "error in \"PRAGMA user_version\": $v"
      exit
    }
    if {$v==$iVersion} {
      puts "...actually waited [expr [clock milliseconds] - $t1]ms..."
      return
    }
    # puts "version=$v"
    after 100 { incr ::user_version_time }
    vwait ::user_version_time
  }
  error "timeout while waiting for version $iVersion"
}

# bcv_sqlite3 ?-uri? ?-container CONTAINER? ?-daemon DAEMON? CMD FILENAME
#
#   This is a wrapper around the normal [sqlite3] command to open
#   a database within container $CONTAINER as made available by 
#   daemon number $DAEMON.
#
swproc bcv_sqlite3 "
  -uri -secure
  -badauth
  -goodauth
  {-container $D(container)} {-daemon 0} 
  cmd file
" {
  global C
  if {[info exists C(secure)]} { set O(-secure) 1 }

  set f [make_filename $O(-container) $file $O(-daemon)]
  if {$O(-uri) || $O(-secure)} {
    set f "file:$f?bcv_container=$O(-container)"
    if {$O(-secure)} {
      append f "&bcv_secure=1"
      if {!$O(-badauth)} { set O(-goodauth) 1 }
    }
    if {$O(-badauth) || $O(-goodauth)} {
      set container $O(-container)
      if {$O(-badauth)} { set container nosuchcontainer }
      append f "&bcv_auth=[make_uri_sas $container]"
    }
  }
  uplevel sqlite3 $cmd $f -uri 1
}

# bcv_azurite_supported
#
#   Return true if version 3.6.0 or newer of [azurite] can be found.
#
proc bcv_azurite_supported {} {
  set rc [catch { exec azurite --version } msg]
  if {$rc} { return 0 }
  foreach {a b c} [split $msg .] {}
  return [expr {$a>3 || ($a==3 && $b==6)}]
}

proc azurite_msg {id fd args} {
  set str [gets $fd]
  puts "AZ$id: $str"
}

array unset ::AZURITE 
proc bcv_azurite {id} {
  set dir "azurite.$id"
  file delete -force $dir
  file mkdir $dir
  
  set fd [open "|azurite --silent --blobPort 0 --queuePort 0 --location $dir"]
  gets $fd
  set str [gets $fd]
  gets $fd 
  gets $fd
  regexp {127.0.0.1:[0-9]+} $str addr
  fconfigure $fd -blocking 0
  fileevent $fd readable [list azurite_msg $id $fd]
  set ::AZURITE($id) $fd
  return $addr
}

proc bcv_azurite_kill {id} {
  set pid [pid $::AZURITE($id)]
  close $::AZURITE($id)
  exec kill -KILL $pid
}

proc execsql_pp {sql {db db}} {
  set nCol 0
  $db eval $sql A {
    if {$nCol==0} {
      set nCol [llength $A(*)]
      foreach c $A(*) { 
        set aWidth($c) [string length $c] 
        lappend data $c
      }
    }
    foreach c $A(*) { 
      set n [string length $A($c)]
      if {$n > $aWidth($c)} {
        set aWidth($c) $n
      }
      lappend data $A($c)
    }
  }
  if {$nCol>0} {
    set nTotal 0
    foreach e [array names aWidth] { incr nTotal $aWidth($e) }
    incr nTotal [expr ($nCol-1) * 3]
    incr nTotal 4

    set fmt ""
    foreach c $A(*) { 
      lappend fmt "% -$aWidth($c)s"
    }
    set fmt "| [join $fmt { | }] |"
    
    puts [string repeat - $nTotal]
    for {set i 0} {$i < [llength $data]} {incr i $nCol} {
      set vals [lrange $data $i [expr $i+$nCol-1]]
      puts [format $fmt {*}$vals]
      if {$i==0} { puts [string repeat - $nTotal] }
    }
    puts [string repeat - $nTotal]
  }
}

#-------------------------------------------------------------------------
#
proc bcv_proxy_test {} {
  set rc [catch { 
    sqlite3 bpt bcvproxy.db -readonly 1
    bpt one {SELECT id FROM proxy_id} 
  } proxyid]
  catch {bpt close}
  if {$rc} {
    error "failed to find bcvproxy.db database"
  }

  set rc [catch { set s [socket 127.0.0.1 10000] } msg]
  if {$rc} {
    error "failed to connect to proxy process at 127.0.0.1:10000"
  }

  catch {
    puts $s "HELLO PROXY"
    flush $s
    gets $s
  } ret
  catch { close $s }

  if {$ret!="HELLO CLIENT MY ID IS $proxyid"} {
    error "proxy process/database mismatch"
  }
}

proc bcv_proxy_config {sql} {
  sqlite3 bpt bcvproxy.db
  bpt eval $sql
  bpt close
}

proc bcv_mkdir {dirname} {
  file delete -force $dirname
  file mkdir $dirname
}

proc test_auth_callback {zStorage zAccount zContainer} {
  set sas [make_sas $zContainer]
  return $sas
}

proc bcvfs_create {dirname vfsname} {
  global C
  set cmd [sqlite3_bcvfs_create $dirname $vfsname]
  if {$C(use_sas)} {
    $cmd auth_callback test_auth_callback
  }
  set cmd
}


proc bcv_upload_empty_database {container dbname} {
  file delete -force empty.db
  sqlite3 emptydb empty.db
  emptydb eval { PRAGMA user_version = 42 }
  emptydb close
  bcv_upload_database -container $container empty.db $dbname
}

swproc bcvfs_attach {
  -noerr -secure -ifnot {-alias {}} {-user {}} {-module {}} vfs container
} {
  set a [list]
  if {$O(-alias)!=""} {
    lappend a -alias $O(-alias)
  } 
  if {$O(-user)==""} {
    set O(-user) $::C(user)
  } 
  if {$O(-module)==""} {
    set O(-module) $::C(module)
  } 
  if {$O(-noerr)} {
    lappend a -noerr
  }
  if {$O(-secure)} {
    lappend a -secure
  }
  if {$O(-ifnot)} {
    lappend a -ifnot
  }
  $vfs attach {*}$a $O(-module) $O(-user) $container
}

proc test_result {default args} {
  upvar F F 

  set errlist [concat $args $F(errlist)]

  set res [list $F(rc) $F(err)]
  set bErr 1
  if {$F(fail)==0} {
    if {$res==$default} {
      set bErr 0
    }
  } else {
    for {set i 0} {$i < [llength $errlist]} {incr i} {
      set candidate [lindex $errlist $i]
      if {$res==$candidate} {
        set bErr 0
        break
      }
    }
  }

  if {$bErr} {
    error "unexpected result: {$res} (fail=$F(fail))"
  }
}

swproc do_fault_test_body {
  {-inject ""}
  {-errorlist ""}
  {-tn ""}
  {-prepare ""}
  {-body ""}
  {-test ""}
  {-start 1}
  {-last 1000000}
} {
  if {$O(-tn)==""} { error "missing required -tn option" }
  if {$O(-inject)==""} { error "missing required -inject option" }
  if {$O(-errorlist)==""} { error "missing required -errorlist option" }

  proc fault_test_test {tn fail rc err errlist} [subst -nocommands {
    upvar F F
    set F(fail) [set fail]
    set F(rc) [set rc]
    set F(err) [set err]
    set F(errlist) [set errlist]
    uplevel [list do_test [set tn] { $O(-test) ; set {} {} } {}]
  }]

  set bCont 1
  for {set iMemFault $O(-start)} {$iMemFault<=$O(-last)} {incr iMemFault} {

    uplevel $O(-prepare)
    $O(-inject) $iMemFault
    set res [list [catch { uplevel $O(-body) } msg] $msg]
    set val [$O(-inject) 0]

    set bCont [expr $val==0]
    foreach {rc msg} $res {}
    uplevel [list fault_test_test $O(-tn).$iMemFault $bCont $rc $msg $O(-errorlist)]

    if {$bCont==0} break
  }

  $O(-inject) 0 
}

proc do_oom_test2 {tn args} {
  eval do_oom_test         $tn.1 $args
  eval do_oom_persist_test $tn.2 $args
}

proc do_oom_test {tn args} {
  set e [list {1 {out of memory}} {1 7}]
  do_fault_test_body -tn $tn -inject bcv_oom_control -error $e {*}$args 
}

proc bcv_oom_persist_control {iFault} {
  if {$iFault} {
    bcv_oom_control $iFault 1
  } else {
    bcv_oom_control $iFault 0
  }
}
proc do_oom_persist_test {tn args} {
  set e [list {1 {out of memory}} {1 7}]
  do_fault_test_body -tn $tn -inject bcv_oom_persist_control -error $e {*}$args 
}

proc do_socket_fault_test {tn args} {
  set e [list {1 10} {1 {disk I/O error}}]
  do_fault_test_body -tn $tn -inject bcv_socket_fault_control -error $e {*}$args
}

proc do_ioerr_test {tn args} {
  set e [list {1 {I/O error}} {1 {disk I/O error}} {1 7}]
  do_fault_test_body -tn $tn -inject bcv_ioerr_control -error $e {*}$args 
}

proc do_fault_test {tn args} {
  do_oom_test "oom-$tn" {*}$args
  do_socket_fault_test "socket-$tn" {*}$args
}

proc do_oom_ioerr_test {tn args} {
  do_ioerr_test "ioerr-$tn" {*}$args
  do_oom_test "oom-$tn" {*}$args
}

proc bcv_upload_new_db {container dbname sql} {
  file delete -force tmpccd.db
  sqlite3 ccd tmpccd.db 
  ccd eval $sql
  ccd close
  bcv_upload_database -container $container tmpccd.db $dbname
  file delete -force tmpccd.db
}

#-------------------------------------------------------------------------
# Run the rest of this script only once.
#
if {[info commands finish_test]!=""} { return }
set ::testprefix ""
set G(nPass) 0
set G(nFail) 0
set G(lFail) [list]

proc finish_test {} {
  global G
  puts "$G(nFail) errors from [expr $G(nFail)+$G(nPass)] tests"
  if {[llength $G(lFail)]} { puts "Failures: $G(lFail)" }

  set nAlloc [lindex [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0] 1]
  set nByte [lindex [sqlite3_status SQLITE_STATUS_MEMORY_USED 0] 1]

  if {$nAlloc>0} { puts -nonewline "ERROR: " }
  puts "$nByte bytes in $nAlloc allocations outstanding"

}

