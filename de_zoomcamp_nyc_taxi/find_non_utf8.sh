#!/bin/bash

find_non_utf8_files() {
    find . -type f | while read file; do
        if ! iconv -f utf-8 -t utf-8 < "$file" &>/dev/null; then
            echo "Non-UTF-8 file found: $file"
            head -n 1 "$file" | xxd -p | sed 's/\(..\)/\\x\1/g'
        fi
    done
}

find_non_utf8_files