import os
sudoPassword = 'change!ictd'
command = 'systemctl restart elasticsearch.service'
os.system('echo %s|sudo -S %s' % (sudoPassword, command))
