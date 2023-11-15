#!/bin/bash -e

CEPH_CONFIG="/etc/ceph/ceph.conf"
MON_CONFIG="/etc/rook/mon-endpoints"
KEYRING_FILE="/etc/ceph/keyring"

dry_run=false
skip_watch=false
while getopts d:s:n:l: flag
do
    case "${flag}" in
        d) dry_run=true ;;
        s) skip_watch=true ;;
        n) namespace=${OPTARG} ;;
        l) list=${OPTARG} ;;
        *) echo 'error in command line parsing' >&2
           exit 1
    esac
done

#echo "param dry_run is: $dry_run" >> volume-migration.log
#echo "param skip_watch: $skip_watch" >> volume-migration.log
echo "namespace is: $namespace" >> volume-migration.log
# create a ceph config file in its default location so ceph/rados tools can be used
# without specifying any arguments
write_endpoints() {
  endpoints=$(cat ${MON_CONFIG})

  # filter out the mon names
  # external cluster can have numbers or hyphens in mon names, handling them in regex
  # shellcheck disable=SC2001
  mon_endpoints=$(echo "${endpoints}"| sed 's/[a-z0-9_-]\+=//g')

  DATE=$(date)
  echo "$DATE writing mon endpoints to ${CEPH_CONFIG}: ${endpoints}"
    cat <<EOF > ${CEPH_CONFIG}
[global]
mon_host = ${mon_endpoints}

[client.admin]
keyring = ${KEYRING_FILE}
EOF
}

# watch the endpoints config file and update if the mon endpoints ever change
watch_endpoints() {
  # get the timestamp for the target of the soft link
  real_path=$(realpath ${MON_CONFIG})
  initial_time=$(stat -c %Z "${real_path}")
  while true; do
    real_path=$(realpath ${MON_CONFIG})
    latest_time=$(stat -c %Z "${real_path}")

    if [[ "${latest_time}" != "${initial_time}" ]]; then
      write_endpoints
      initial_time=${latest_time}
    fi

    sleep 10
  done
}

# read the secret from an env var (for backward compatibility), or from the secret file
ceph_secret=${ROOK_CEPH_SECRET}
if [[ "$ceph_secret" == "" ]]; then
  ceph_secret=$(cat /var/lib/rook-ceph-mon/secret.keyring)
fi

# create the keyring file
cat <<EOF > ${KEYRING_FILE}
[${ROOK_CEPH_USERNAME}]
key = ${ceph_secret}
EOF

# write the initial config file
write_endpoints

# Run volume migration script
if [ "$dry_run" = true ]; then
        echo "In dry run mode" >> volume-migration.log
        ./volume-migration-script.sh -d dry_run -n $namespace -l "$list" >> volume-migration.log
else
        ./volume-migration-script.sh -n $namespace -l "$list" >> volume-migration.log
fi

# continuously update the mon endpoints if they fail over
if [ "$skip_watch" != true ]; then
        watch_endpoints
fi
