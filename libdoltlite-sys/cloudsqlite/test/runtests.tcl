
set dirname [file dirname [info script]]
source [file join $dirname config.tcl]

#-------------------------------------------------------------------------

# If test_common.tcl has already run, variable $bcv_test_common_tcl is
# already set. In this case populate the C() array with the default test 
# config and return early.
#
if {[info exists bcv_test_common_tcl]} {
  if {[info exists C(use_sas)]==0} {
    array set C $bcv_config($bcv_default_config)
    set testsuite $bcv_default_config
  }
  return
}

set dirname [file dirname [info script]]
source [file join $dirname test_common.tcl]

rename finish_test really_finish_test
proc finish_test {} {}

swproc runtests_main {{-config ""} {-start ""} {pattern *}} {
  global dirname
  global bcv_testsuite_list
  global C

  set bSeen 0
  if {$O(-start)==""} {
    set bSeen 1
  }

  set files [list]
  foreach f [glob -dir $dirname bcvfs*.test dmn*.test util*.test] {
    set file [file tail $f]
    if {$bSeen==0 && $file!=$O(-start)} continue
    set bSeen 1
    if {[string match $pattern $file]==0} continue
    if {([string match *large* $file]==0 && [string match *proxy* $file]==0)
      || $pattern!="*"
    } {
      lappend files $file
    }
  }

  set lSuite [list]
  if {$O(-config)!=""} {
    set mat [string map {% * _ ?} $O(-config)]
    foreach x [array names ::bcv_config] {
      if {[string match $mat $x]} { lappend lSuite $x }
    }
  } else {
    set lSuite $bcv_testsuite_list
  }

  foreach t $lSuite {
    array unset C
    array set C $::bcv_config($t)
    set ::testsuite $t

    set fset $files
    if {$pattern=="*" && [info exists C(tests)]} {
      set fset $C(tests);
    }
    foreach f $fset { 
      set t [lindex [time { uplevel #0 source [file join $dirname $f] }] 0]
      set s [format %.2f [expr $t / 1000000.0]]
      puts "FILE $f: ${s}s"
    }
  }
}

runtests_main {*}$argv
really_finish_test

