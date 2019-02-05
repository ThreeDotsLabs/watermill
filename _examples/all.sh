#!/bin/bash

# update_gomod finds all the go.mod enabled subprojects and updates their dependency
# to a specified version.
function update_gomod() {
        dependency="$1"
        if [ -z "$dependency" ]; then dependency="github.com/ThreeDotsLabs/watermill"; fi
        revision="$2"
        if [ -z "$revision" ]; then revision="master"; fi

        echo "Setting $dependency to $revision..."

        for gomod in $(find . -name "go.mod")
        do
                dir="$(realpath --relative-to=$(pwd) $(dirname "$gomod"))"
                pushd "$(realpath $(dirname "$gomod"))" &> /dev/null

                # skip files that currently have no dependency
                if grep -q "$dependency" ./go.mod
                then
                        echo -e "\t$dir"
                        go get "$dependency@$revision" 2> /dev/null
                fi

                popd &> /dev/null
        done

        echo "...done"
}
        
# Update the gomod files for every example
update_gomod


# Run all the tests in this directory tree
anyError=0
for batsFile in $(find . -name "*.bats")
do
        echo -e "$batsFile\n"
        bats $batsFile || anyError=1
done

exit $anyError
