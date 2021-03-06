from DataPype.etc import slack_post as sp
import subprocess

mariadb=subprocess.check_output(['systemctl','status','mariadb'], text=True)
mariadb=str(a[0:-1])
print(mariadb)
