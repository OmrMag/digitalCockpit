#!/bin/bash
# Create required folders and placeholder files if they don't exist
mkdir -p exports

if [ ! -f config.json ]; then
  echo '{"morta":{"api_key":"","hub_id":""},"revizto_dubai":{"default_license_role":2,"access_code":""},"revizto_ireland":{"default_license_role":2,"access_code":""},"forma":{"client_id":"","client_secret":"","client_id_3l":"","client_secret_3l":"","account_id":"","project_id":"","admin_user_id":""},"projects":{"morta":[],"revizto":[],"forma":[]},"active_context":{"morta_hub_id":"","morta_hub_name":"","revizto_project_uuid":"","revizto_project_name":"","revizto_region":"","forma_project_id":"","forma_project_name":"","forma_platform":""}}' > config.json
fi

if [ ! -f users.json ]; then
  echo '[]' > users.json
fi

if [ ! -f invite_log.json ]; then
  echo '[]' > invite_log.json
fi

if [ ! -f portal_requests.json ]; then
  echo '[]' > portal_requests.json
fi

# Start the app
uvicorn app:app --host 0.0.0.0 --port 8000
