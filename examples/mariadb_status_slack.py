
import DataPype.etc as etc
import subprocess
#get mariadb status report to slack:
mariadb=subprocess.check_output(['systemctl','status','mariadb'], text=True)
mariadb=str(mariadb[0:-1])
#write to Slack:
mariadb='```'+mariadb+'```'
etc.slack_post(message=mariadb, channel='sys-status-report', user='sysadmin-bot', webhook='https://hooks.slack.com/services/something')
#lets check for users too:
users=subprocess.check_output(['who'], text=True)
users=str(users[0:-1])
#write to Slack again:
users='```'+users+'```'
etc.slack_post(message=users, channel='sys-status-report', user='sysadmin-bot', webhook='https://hooks.slack.com/services/something')
