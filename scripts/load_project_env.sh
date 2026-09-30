#!/usr/bin/env bash

# Load simple KEY=VALUE entries from the project-local .env file without
# replacing variables explicitly supplied by the caller.
env_file="${1:-.env}"
[[ -f "$env_file" ]] || return 0

while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%$'\r'}"
  [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue

  key="${line%%=*}"
  value="${line#*=}"
  [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
  [[ -n "${!key+x}" ]] && continue

  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  export "$key=$value"
done < "$env_file"

unset env_file line key value
