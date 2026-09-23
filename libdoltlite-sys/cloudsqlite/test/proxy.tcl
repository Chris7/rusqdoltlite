
# Commands for action column:
#
#    delay MS
#    oneerror STATUS REASON HDR
#    error    STATUS REASON HDR
#
#    cnterror IGNORECOUNT STATUS REASON HDR
#

# Contants:
set ::LISTEN_PORT 10000
set ::SERVER_HOST 127.0.0.1
set ::SERVER_PORT 11000
set ::DATABASE "bcvproxy.db"

unset -nocomplain one_error_array
unset -nocomplain cnt_error_array

#-------------------------------------------------------------------------

# Open the configuration database
sqlite3 db $::DATABASE
db eval {
  CREATE TABLE IF NOT EXISTS new_client_message(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern,          -- LIKE pattern for $line1
    action            -- Action to take. e.g. "delay 100"
  );

  CREATE TABLE IF NOT EXISTS proxy_id(
    id TEXT
  );
  DELETE FROM proxy_id;
  INSERT INTO proxy_id VALUES(random());
}

# Listen on the socket for new HTTP requests.
socket -server new_connection $::LISTEN_PORT
puts "Listening on 127.0.0.1:$::LISTEN_PORT"

proc new_connection {s clientAddr clientPort} {
  recv_http_message $s 0 [list new_client_message $s]
}

proc new_client_message {s line1 headers body {first_action 0}} {
  puts "client->proxy: $line1"
  set bForward 1

  db eval {
    SELECT rowid AS rowid, action FROM new_client_message 
    WHERE $line1 LIKE pattern AND rowid>$first_action
    ORDER BY rowid
  } {

    switch -- [lindex $action 0] {
      delay {
        set nMs [lindex $action 1]
        set p1 [expr $rowid+1]
        after $nMs [list new_client_message $s $line1 $headers $body $p1]
        set bForward 0
      }

      partial {
        set bForward 0
        set script [list new_partial_reply [lindex $action 1]]
        forward_client_message $s $line1 $headers $body $script
      }

      stallafter {
        set bForward 0
        set script [list new_stallafter_reply [lindex $action 1]]
        forward_client_message $s $line1 $headers $body $script
      }

      oneerror {
        global one_error_array
        if {[info exists one_error_array($line1)]} {
          unset one_error_array($line1)
        } else {
          set hdrlist {}
          foreach {status reason hdrlist} [lrange $action 1 end] {}
          set one_error_array($line1) 1
          send_http_message $s "HTTP/1.1 $status $reason" $hdrlist {}
          puts "proxy->client: HTTP/1.1 $status $reason"
          close $s
          set bForward 0
        }
      }

      nerror {
        global nerror_array
        set tail [lrange $action 1 end]
        if {[llength $tail]!=4} {
          error "wrong number of args for nerror"
        }
        foreach {cnt status reason hdrlist} $tail {}

        set key "$line1,$rowid"
        if {![info exists nerror_array($key)]} {
          set nerror_array($key) $cnt
        }
        if {$nerror_array($key)!=0} {
          set hdrlist {}
          send_http_message $s "HTTP/1.1 $status $reason" $hdrlist {}
          puts "proxy->client: HTTP/1.1 $status $reason"
          close $s
          set bForward 0
          incr nerror_array($key) -1
        }
      }

      error {
        set hdrlist {}
        foreach {status reason hdrlist} [lrange $action 1 end] {}
        send_http_message $s "HTTP/1.1 $status $reason" $hdrlist {}
        puts "proxy->client: HTTP/1.1 $status $reason"
        close $s
        set bForward 0
      }

      cnterror {
        global cnt_error_array
        set tail [lrange $action 1 end]
        if {[llength $tail]!=4} {
          error "wrong number of args for cnterror"
        }
        foreach {cnt status reason hdrlist} $tail {}
        if {[info exists cnt_error_array($rowid)]} {
          incr cnt_error_array($rowid)
        } else {
          set cnt_error_array($rowid) 0
        }

        if {$cnt_error_array($rowid)==$cnt} {
          send_http_message $s "HTTP/1.1 $status $reason" $hdrlist {}
          puts "proxy->client: HTTP/1.1 $status $reason"
          close $s
          set bForward 0
        }
      }

      default {
        error "unrecognized action in \"$action\""
      }
    }
  }

  if {$bForward} {
    forward_client_message $s $line1 $headers $body
  }
}

proc forward_client_message {s line1 headers body {script new_server_reply}} {
  set s2 [socket $::SERVER_HOST $::SERVER_PORT]
  send_http_message $s2 $line1 $headers $body
  puts "proxy->server: $line1"
  set bHead 0
  if {[string toupper [lindex $line1 0]]=="HEAD"} {
    set bHead 1
  }
  recv_http_message $s2 $bHead [list {*}$script $s2 $s]
}

proc new_server_reply {s2 s line1 headers body} {
  puts "server->proxy: $line1"

  send_http_message $s $line1 $headers $body
  puts "proxy->client: $line1"

  set status [lindex [split $line1] 1]
  if {$status==100} {
    recv_http_message $s2 0 [list new_server_reply $s2 $s]
  } else {
    close $s2
    close $s
  }
  # recv_http_message $s [list new_client_message $s]
}

proc new_partial_reply {nByte s2 s line1 headers body} {
  new_server_reply $s2 $s $line1 $headers [string range $body 0 $nByte-1]
}

proc new_stallafter_reply {nByte s2 s line1 headers body} {
  puts "server->proxy: $line1"

  set newbody [string range $body 0 $nByte-1]
  send_http_message $s $line1 $headers $newbody
  puts "proxy->client: $line1"

  set status [lindex [split $line1] 1]
  if {$status==100} {
    recv_http_message $s2 0 [list new_stallafter_reply $s2 $s]
  } else {
  puts "shutdown $s"
    fileevent $s readable {}
    close $s2
  }
}

#--------------------------------------------------------------------------
# Parameter $s is a connected socket on which an HTTP message is about
# to be received. This command sets up [fileevent] callbacks to receive
# and process the message. Once the message is received, three elements
# are appended to Tcl script $script and the result evaluated: 
#
#     eval $script [list $line1 $headers $body]  
#
# $line1:
#   The first line of the HTTP message. e.g. "PUT /uri HTTP/1.1"
#
# $headers:
#   List of an even number of elements. Each pair represents a single
#   HTTP header. The first element of each pair is the header name, the
#   second the value. e.g.
#
#     {Content-Type application/octet-stream Content-Length 24}
#
# $body:
#   The body of the request.
#
proc recv_http_message {s bHead script} {
  global RHM 

  fconfigure $s -blocking 0 -translation binary
  set RHM($s) [dict create state 0 head $bHead] 

  fileevent $s readable [list recv_http_message_cb $s $script]
}

proc recv_http_message_cb {s script} {
  global RHM
  switch -- [dict get $RHM($s) state] {
    0 {
      # Waiting for the request line

      set line [string trimright [gets $s]]

      if {[eof $s]} {
        unset RHM($s)
        close $s
        return
      }
      if {$line=="HELLO PROXY"} {
        puts $s "HELLO CLIENT MY ID IS [db one {SELECT id FROM proxy_id}]"
        close $s
        return
      }

      dict set RHM($s) request $line
      dict set RHM($s) headers [list]
      dict set RHM($s) body ""

      dict set RHM($s) rem 0
      dict set RHM($s) state 1
    }

    1 {
      # Reading header field lines

      set line [string trimright [gets $s]]
      if {$line==""} {
        if {[dict get $RHM($s) rem]<0} {
          dict set RHM($s) state 3
        } elseif {[dict get $RHM($s) rem]==0} {
          dict set RHM($s) state 4
        } else {
          dict set RHM($s) state 2
        }
      } else {
        set ii [string first : $line]
        if {$ii<0} { error "Error parsing http header: $line" }
        set name  [string trim [string range $line 0 $ii-1]]
        set value [string trim [string range $line $ii+1 end]]

        dict set RHM($s) headers $name $value
        if {[dict get $RHM($s) head]==0} {
          if {[string tolower $name]=="content-length"} {
            dict set RHM($s) rem $value
          }
          if {[string tolower $name]=="transfer-encoding"
           && [string tolower $value]=="chunked"
          } {
            dict set RHM($s) rem -1
          }
        }
      }
    }

    2 {
      # Reading the body. Dict element "rem" has the number of 
      # bytes left to read. 
      set rem [dict get $RHM($s) rem]
      set data ""
      if {$rem>0} { set data [read $s $rem] }

      set body [dict get $RHM($s) body]
      append body $data
      dict set RHM($s) body $body

      incr rem [expr -1 * [string length $data]]
      dict set RHM($s) rem $rem
      if {$rem<=0} {
        dict set RHM($s) state 4
      }
    }

    3 {
      # Reading a Transfer-Encoding: chunked header.
      set body [dict get $RHM($s) body]

      set line [string trim [gets $s]]
      append body "$line\r\n"
      if {$line==""} {
        set line [string trim [gets $s]]
        append body "$line\r\n"
      }
      set nByte [expr "0x$line"]
      #puts "TRANSFER: nByte=$nByte"
      if {$nByte>0} {
        append body [read $s [expr $nByte]]
      } else {
        dict set RHM($s) state 4
        append body "\r\n"
      }

      dict set RHM($s) body $body
    }
  }

  if {[dict get $RHM($s) state]>=4} {
    set line1 [dict get $RHM($s) request]
    set headers [dict get $RHM($s) headers]
    set body [dict get $RHM($s) body]
    unset RHM($s)
    eval $script [list $line1 $headers $body]
  }
}

proc send_http_message {s line1 headers body} {
  fconfigure $s -blocking 0 -translation binary

  # puts #########################################
  # puts $line1
  # puts $headers
  # puts $body
  # puts "length of body: [string length $body]"
  # puts #########################################
  
  puts -nonewline $s "$line1\r\n"
  foreach {k v} $headers {
    if {[string tolower $k]=="content-length" && [string length $body]==0} {
      set v [string length $body]
    }
    puts -nonewline $s "$k: $v\r\n"
  }
  puts -nonewline $s "\r\n"
  puts -nonewline $s $body
  flush $s

set s [open /tmp/out w]
  puts -nonewline $s "$line1\r\n"
  foreach {k v} $headers {
    if {[string tolower $k]=="content-length" && [string length $body]==0} {
      set v [string length $body]
    }
    puts -nonewline $s "$k: $v\r\n"
  }
  puts -nonewline $s "\r\n"
  puts -nonewline $s $body
  flush $s
  close $s
}

vwait forever

