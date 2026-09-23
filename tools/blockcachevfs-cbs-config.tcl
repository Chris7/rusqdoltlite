# Configuration overlay for the upstream Cloud Backed SQLite Tcl suite.
# The runner copies this file to a disposable checkout as test/config.tcl and
# sets BLOCKCACHEVFS_CBS_BASE_CONFIG to the original upstream config.tcl.
source $::env(BLOCKCACHEVFS_CBS_BASE_CONFIG)

proc rusqdoltlite_static_auth {bReadonly container {seconds 0}} {
  global C
  return $C(auth)
}

set _tests {
  util_api1.test
  util_upload2.test
  bcvfs_poll1.test
}

set _configs {}

if {[info exists ::env(BLOCKCACHEVFS_S3_ENDPOINT)]
    && $::env(BLOCKCACHEVFS_S3_ENDPOINT)!=""} {
  set _s3_bucket $::env(BLOCKCACHEVFS_S3_BUCKET)
  set _s3_region $::env(BLOCKCACHEVFS_S3_REGION)
  set _s3_user $::env(BLOCKCACHEVFS_S3_ACCESS_KEY)
  set _s3_secret $::env(BLOCKCACHEVFS_S3_SECRET)
  set _s3_endpoint $::env(BLOCKCACHEVFS_S3_ENDPOINT)
  set bcv_config(rusqdoltlite_s3) [list \
    storage s3 \
    user $_s3_user \
    auth $_s3_secret \
    use_sas 1 \
    sas_cmd rusqdoltlite_static_auth \
    module "s3?region=${_s3_region}&endpoint=${_s3_endpoint}" \
    inlinecmd 1 \
    namebytes 16 \
    blocksize 131072 \
    postfix "" \
    tests $_tests]
  lappend _configs rusqdoltlite_s3
}

if {[info exists ::env(BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT)]
    && $::env(BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT)!=""} {
  set _google_bucket $::env(BLOCKCACHEVFS_GOOGLE_BUCKET)
  set _google_project $::env(BLOCKCACHEVFS_GOOGLE_PROJECT)
  set _google_token $::env(BLOCKCACHEVFS_GOOGLE_TOKEN)
  set _google_endpoint $::env(BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT)
  set bcv_config(rusqdoltlite_google_json) [list \
    storage google \
    user $_google_project \
    auth $_google_token \
    use_sas 1 \
    sas_cmd rusqdoltlite_static_auth \
    module "google?api=json&endpoint=${_google_endpoint}" \
    inlinecmd 1 \
    namebytes 16 \
    blocksize 131072 \
    postfix "" \
    tests $_tests]
  lappend _configs rusqdoltlite_google_json
}

if {[llength $_configs]==0} {
  error "set BLOCKCACHEVFS_S3_ENDPOINT or BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT"
}

set bcv_testsuite_list $_configs
set bcv_default_config [lindex $_configs 0]
array set ::C $bcv_config($bcv_default_config)
