#set up the example 'mariadb_status_slack.py' as a systemd service running hourly
import .DataPype.etc as etc

# Name of the service. Make it descriptive.
target = mariadb_status_slack

# Description for the service. Recommended, since this is read by GUI admin panels like Cockpit
description = 'Notifies Slack of mariadb status, users connected'

# Time information. Unneeded since 'hourly' is used.
#time = '03:00:00'
#day = 'sunday'

# User to run the service as.
user = 'peaceblaster'
etc.systemdSchedule(target, description=description, hourly=True, user=user)
