#!/bin/bash

echo "Duplicated source files:"
awk '{ print $1 }' "${@}" | sort | uniq -d
echo ""

echo "Duplicated dest files:"
awk '{ print $2 }' "${@}" | sort | uniq -d
echo""
