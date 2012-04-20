# we can use CEPH_CONF to override the normal configuration file location.
  $ env CEPH_CONF=from-env ceph-conf -s foo bar
  global_init: unable to open config file. (re)
  [1]

# command-line arguments should override environment
  $ env -u CEPH_CONF ceph-conf -c from-args
  global_init: unable to open config file. (re)
  [1]
