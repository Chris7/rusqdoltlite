# Default configuration is to assume that azurite (Azure storage emulator)
# is running on the default port (10000) on the localhost. The account-name
# and access-key below are well known values built into the emulator.
#set bcv_default_config azure_emu2
#set bcv_default_config azure_emu_sas_secure
set bcv_default_config azure_emu_sas
#set bcv_default_config azure_emu

# Test suites run if this script is invoked with no arguments.
#
set bcv_testsuite_list { 
  azure_emu
  azure_emu_sas 
}
# azure_emu_sas 


set    DEFAULTAZUREKEY Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6
append DEFAULTAZUREKEY IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

set bcv_config(azure_emu_sas) {
  storage     azure

  accesskey   $DEFAULTAZUREKEY

  auth        ""

  user        devstoreaccount1
  use_sas     1
  sas_cmd     "make_azure_sas"
  sas_cmd2    "make_azure_create_sas"

  inlinecmd   1
  namebytes   16
  module      "azure?emulator=127.0.0.1:10000&sas=1"
}

set bcv_config(azure_emu) {
  storage     azure

  user        devstoreaccount1
  account     devstoreaccount1
  accesskey   $DEFAULTAZUREKEY
  auth        $DEFAULTAZUREKEY

  use_sas     0

  namebytes   24
  blocksize   131072
  module      "azure?emulator=127.0.0.1:10000"
  inlinecmd   1
  secret      ""
}

if {[file exists ../real_azure_account.tcl]} {
  source ../real_azure_account.tcl
  # set bcv_default_config azure_real_sas
}

#-------------------------------------------------------------------------
foreach k [array names bcv_config] {
  set bcv_config($k) [subst -nocommands $bcv_config($k)]
}

array set ::C $bcv_config($bcv_default_config)

#-------------------------------------------------------------------------
# Usage:
#   
proc swproc {name arglist body} {
  set T [list]
  set L [list]
  set B [list]
  foreach elem $arglist {
    if {[llength $elem]==2 && [string range [lindex $elem 0] 0 0]=="-"} {
      lappend L [lindex $elem 0] [lindex $elem 1]
    } elseif {[llength $elem]==1 && [string range $elem 0 0]=="-"} {
      lappend L $elem 0
      lappend B $elem
    } else {
      lappend T $elem
    }
  }

  proc $name {args} [subst -nocommands {
    array set O {$L}
    set T [swproc_martial_args [set args] {$B}]
    ${name}_impl {*}[set T]
  }]

  proc ${name}_impl $T [subst {
    upvar O O
    $body
  }]
}

proc swproc_martial_args {lArg lBool} {
  upvar O O
  set ret [list]
  for {set i 0} {$i < [llength $lArg]} {incr i} {
    set a [lindex $lArg $i]
    if {[string range $a 0 0]=="-"} {
      set lCand [array names O ${a}*]
      if {[llength $lCand]==0} {
        error "no such option: $a"
      } elseif {[llength $lCand]>1} {
        error "ambigous option: $a (could be any of [join $lCand {, }])"
      } else {
        set opt [lindex $lCand 0]
        if {[lsearch $lBool $opt]>=0} {
          set O($opt) 1
        } else {
          incr i
          if {$i==[llength $lArg]} {
            error "option requires an argument: $a"
          }
          set O($opt) [lindex $lArg $i]
        }
      }
    } else {
      lappend ret $a
    }
  }
  return $ret
}
#-------------------------------------------------------------------------


