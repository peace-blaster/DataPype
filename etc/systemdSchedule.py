# systemdSchedule
# creates a systemd service for target python script, makes systemd timer to user specifications
# WARNING! Your sysadmin probably won't want you doing this, and it will require sudo permissions on the system

import os

def systemdSchedule(target, description='description not provided', time=False, day=False, hourly=False, daily=False, weekly=False, pythonPath='/usr/bin/python3', user='', path=''):
    # make the file for the systemd timer
    #parse through input to make 'OnCalendar' line:
    if hourly:
        if daily or weekly:
            raise InputError('too many modes selected- cannot be hourly and daily or weekly')
            return False
        timeString='OnCalendar=hourly'
    if daily:
        if hourly or weekly:
            raise InputError('too many modes selected- cannot be daily and hourly or weekly')
            return False
        timeString='OnCalendar=daily'
        if time:
            timeString='OnCalendar=*-*-* time'
    if weekly:
        if hourly or weekly:
            raise InputError('too many modes selected- cannot be weekly and hourly or daily')
            return False
        if day:
            day=str(day)
            if day.lower()=='friday':
                day='Fri'
            if day.lower()=='saturday':
                day='Sat'
            if day.lower()=='sunday':
                day='Sun'
            if day.lower()=='monday':
                day='Mon'
            if day.lower()=='tuesday':
                day='Tue'
            if day.lower()=='wednesday':
                day='Wed'
            if day.lower()=='thursday':
                day='Thu'
            if time:
                time=str(time)
            else:
                time='00:00:00'
        else:
            day='Sun'
        timeString='OnCalendar={day} *-*-* {time}'.format(day=day, time=time)
    #print out final product:
    timerText="""
        [Unit]
        Description={desc}

        [Timer]
        {time}
        RandomizedDelaySec=7200

        Persistent=true

        [Install]
        WantedBy=timers.target""".format(desc=description, time=timeString)
    #make service
    # defaulting to 'no' on restart because a data pipeline probably shouldn't
    serviceText="""
            [Unit]
            Description={desc}
            WantedBy=multi-user.target
            [Service]
            Type=simple
            ExecStart={pypath} {path}
            User={user}
            Restart=no""".format(desc=description, pypath=pythonPath, path=path, user=user)
    #write files:
    service=open('/etc/systemd/system/{target}.service'.format(target=target),'w+')
    service.write(serviceText)
    service.close()
    timer=open('/etc/systemd/system/{target}.timer'.format(target=target),'w+')
    timer.write(timerText)
    timer.close()
    #refresh and enable- should be run with 'sudo', or as root
    os.system('systemctl daemon-reload')
    os.system('systemctl enable --now {target}.timer'.format(target=target+'_timer'))
