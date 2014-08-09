from datetime import datetime
import csv
import re
import sqlite3 as sql

skill_path = 'C:/Users/Jason/Documents/wurm/players/joedobo/logs/_Skills.2014-03.txt'
event_path = 'C:/Users/Jason/Documents/wurm/players/joedobo/logs/_Event.2014-03.txt'


class SkillLogTimeStamps:
    def __init__(self, skill_log_path):
        self.iter_counter = 1
        self.skill_log_path = skill_log_path
        self.log_position = [0]
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()
        self.datetime_list = [datetime.min, datetime.min]

    def __iter__(self):
        return self

    def __next__(self):
        with open(self.skill_log_path, encoding='utf-8') as fp:
            fp.seek(0, 2)
            if fp.tell() == self.log_position[0]:
                raise StopIteration
            fp.seek(self.log_position[0])
            self.line_list = list()
            for line in iter(fp.readline, ''):
                self.log_position.insert(0, fp.tell())
                self.log_position = self.log_position[:2]
                line = line.strip()
                date1, time1 = time_stamp(line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                if self.date_part.year == 1:
                    continue
                self.datetime_list.insert(0, datetime.combine(self.date_part, self.time_part))
                if self.datetime_list[1] == self.datetime_list[0] or self.datetime_list[1] == datetime.min:
                    self.line_list.insert(0, line)
                    continue
                else:
                    self.log_position = self.log_position[1:]
                break
    pass


class EventLog:
    def __init__(self, event_log_path):
        self.event_log_path = event_log_path
        self.log_position = [0, 0]
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()
        self.datetime_whole = datetime
        self.match_time = datetime.min
        self.line_list = list()

    def __iter__(self):
        return self

    def __next__(self):
        with open(self.event_log_path, encoding='utf-8') as fp:
            fp.seek(self.log_position[0])
            for line in iter(fp.readline, ''):
                self.log_position.insert(0, fp.tell())
                self.log_position = self.log_position[:2]
                line = line.strip()
                #print(line)
                date1, time1 = time_stamp(line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                if self.date_part.year == 1:
                    continue
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.match_time >= self.datetime_whole:
                    self.line_list.insert(0, [self.datetime_whole, line])
                if self.match_time < datetime.combine(self.date_part, self.time_part):
                    self.log_position[0] = self.log_position[1]
                    break


def time_stamp(arg1):
    """
    Convert a string timestamp to datetime object.

    'arg1' is a string containing a time stamp.
    """
    if 'Logging' in arg1[:7]:
        _date_part = datetime.strptime(arg1[16:].strip(), "%Y-%m-%d").date()
        return _date_part, None
    try:
        _time_part = datetime.strptime(arg1[:10], "[%H:%M:%S]").time()
    except ValueError:
        return None, None
    return None, _time_part


def get_regex(sql_arg):
    try:
        sql_arg.execute('''CREATE TABLE regex_look(regex TEXT, outcome TEXT)''')
    except sql.Error:
        pass
    with open("regex.csv", encoding='utf-8', newline='') as fp:
        csv_reader = csv.reader(fp)
        # b'\xe2\x99\xa0' = ♠ for utf-8
        for entries in csv_reader:
            sql_arg.execute('INSERT into regex_look VALUES (?,?)', (entries[0], entries[1]))
            #print(entries)
    return sql_arg


sk = SkillLogTimeStamps(skill_path)
ev = EventLog(event_path)

con = sql.connect("regex.sqlite")
con.isolation_level = None

get_regex(con)


for _ in sk:
    regex_matches = []
    ev.match_time = sk.datetime_list[1]
    next(ev)
    found = False
    with open('log.txt', mode='at') as fp:
        fp.write('\n{}\n'.format(repr(sk.line_list)), )

    index = 0
    for i in ev.line_list[:]:
        found = False
        with con:
            for row in con.execute('''SELECT regex, outcome FROM regex_look'''):
                result = re.search(row[0], i[1])
                if result is not None:
                    found = True
                    if row[1] == 'start' and i[0] == ev.match_time:
                        index += 1
                        break
                    #print('line: {}\nsearch: {}'.format(i, row))
                    regex_matches.insert(0, 'line: {}\nsearch: {}\n'.format(i, repr(row)))
                    ev.line_list.pop(index)
                    break
        if found is False:
            ev.line_list.pop(index)
    else:
        pass
    with open('log.txt', mode='at') as fp:
        for i in regex_matches:
            fp.write('{}'.format(i))