#!/bin/bash

set +H # disable history expansion for using "!" in queries

function create_lock {
  check_pause
  locked=$(fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "if (!Collection.byName('test_lock').exists()) { \
    Collection.create({name:'test_lock'}); \
    true; } else { \
    false; }")
  
  if [ "$locked" != "true" ]; then
    return 1
  else
    return 0
  fi
}

function delete_lock {
  fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "test_lock.definition.delete()"
}

function check_pause {
  while test -e "$FAUNA_QAHOST.pause"; do
    echo "Pausing 30s due to squelched host, $FAUNA_QAHOST"
    sleep 30
  done
}

function main_loop {
  # exit immediately on any failure
  set -e

  local devkey=$1
  local prodkey=$2
  local keys=("$devkey" "$prodkey")

  # init wildcard schema collection
  for key in "${keys[@]}"; do
    check_pause
    fauna eval --url=$FAUNA_ENDPOINT --secret=$key "if (!Collection.byName('test_schema').exists()) \
      Collection.create({name:'test_schema', \
      indexes:{IndexOneField:{values:[{field:'.v1_string'}]}}})"
    fauna eval --url=$FAUNA_ENDPOINT --secret=$key "Set.sequence(1,10).forEach(x => test_schema.create({v1_string:\"x=#{x} at v0\",v1_number:123.456,v1_boolean:false,v1_twotypes:'v0 string'}))"
  done

  sleep 60

  # push schema v1 to dev and prod
  check_pause
  fauna schema push --force --url=$FAUNA_ENDPOINT --secret=$devkey --dir=./schema/1
  fauna schema push --force --url=$FAUNA_ENDPOINT --secret=$prodkey --dir=./schema/1

  sleep 60

  # iterate schema versions on dev
  for version in {2..4}; do
    check_pause
    fauna schema push --force --url=$FAUNA_ENDPOINT --secret=$devkey --dir=./schema/$version
    sleep 60
  done

  # leap-frog prod schema to v4
  check_pause
  fauna schema push --force --url=$FAUNA_ENDPOINT --secret=$prodkey --dir=./schema/4

  sleep 60

  set +e
}

function cleanup {
  local devkey=$1
  local prodkey=$2

  # cleanup
  check_pause
  fauna eval --url=$FAUNA_ENDPOINT --secret=$devkey "if (Collection.byName('test_schema').exists()) test_schema.definition.delete()"
  fauna eval --url=$FAUNA_ENDPOINT --secret=$prodkey "if (Collection.byName('test_schema').exists()) test_schema.definition.delete()"
}

# race to see which "Customer" creates the lock collection first;
# the winner does all the schema stuff
if create_lock; then
  devkey=$(fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "if (!Database.byName('testdb_dev').exists()) \
    Database.create({name:'testdb_dev'}); \
    Key.create({role:'admin',database:'testdb_dev'})" | jq -r .secret)
  prodkey=$(fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "if (!Database.byName('testdb_prod').exists()) \
    Database.create({name:'testdb_prod'}); \
    Key.create({role:'admin',database:'testdb_prod'})" | jq -r .secret)

  keys=("$devkey" "$prodkey")

  attempts=0
  max_attempts=5

  # cleanup collections to start
  cleanup "$devkey" "$prodkey"

  while true; do
    if ! main_loop "$devkey" "$prodkey"; then
      ((attempts++))
      echo "** Main schema loop failed; attempt $attempts out of $max_attempts"

      if [ "$attempts" -ge "$max_attempts" ]; then
        echo "** No retries remaining; cleaning up and exiting"
        cleanup "$devkey" "$prodkey"
        delete_lock
        exit 1
      fi
    fi

    cleanup "$devkey" "$prodkey"
  done

else
  # give the race winner a chance to do setup
  sleep 5

  # create keys since the ones above aren't in scope
  check_pause
  devkey=$(fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "if (Database.byName('testdb_dev').exists()) \
    Key.create({role:'admin',database:'testdb_dev'})" | jq -r .secret)
  prodkey=$(fauna eval --url=$FAUNA_ENDPOINT --secret=$FAUNA_SECRET "if (Database.byName('testdb_prod').exists()) \
    Key.create({role:'admin',database:'testdb_prod'})" | jq -r .secret)

  node index.js $devkey $prodkey
fi
