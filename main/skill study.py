from datetime import datetime, timedelta
from collections import namedtuple
import csv
import sqlite3 as sql

Line = namedtuple('Line', ['time', 'line'])


class SkillLog:
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
                if self.datetime_list[1] == self.datetime_list[0] \
                        or self.datetime_list[0] == self.datetime_list[1] + timedelta(seconds=1) \
                        or self.datetime_list[1] == datetime.min:
                    self.line_list.insert(0, line)
                    continue
                else:
                    self.log_position = self.log_position[1:]
                break
    pass


class EventLog:
    def __init__(self, event_log_path, _start_date):
        self.event_log_path = event_log_path
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()
        self.datetime_whole = datetime
        self.line_list = list()
        self.start_date = _start_date
        self.log_position = list()
        self.log_position[0] = self.get_log_position()

    def __iter__(self):
        return self

    def __next__(self):
        lines_counter = 20
        with open(self.event_log_path, encoding='utf-8') as fp:
            fp.seek(self.log_position[0])
            for line in iter(fp.readline, ''):
                self.log_position[0] = fp.tell()
                line = line.strip()
                date1, time1 = time_stamp(line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                self.line_list.append(Line(self.datetime_whole, line[11:]))
                #print('tell: {}'.format(self.log_position))
                #print(self.line_list[20 - lines_counter])
                lines_counter -= 1
                if lines_counter <= 0:
                    break

    def get_log_position(self):
        with open(self.event_log_path, encoding='utf-8') as fp:
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
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.start_date == self.datetime_whole:
                    return self.log_position[1]

    def line_consumer(self, line_count):
        for _ in range(line_count):
            try:
                _line = self.line_list.pop(0)
            except IndexError:
                raise StopIteration
            yield _line

    pass


class UserData:
    def __init__(self, file):
        self.skill_path, self.event_path, self.start_date, self.end_date = self.fetch_my_data(file)
        if type(eval(self.start_date)) is datetime:
            self.start_date = eval(self.start_date)
        else:
            raise ValueError
        if type(eval(self.end_date)) is datetime:
            self.end_date = eval(self.end_date)
        else:
            raise ValueError

    @staticmethod
    def fetch_my_data(file):
        user_dict = {}
        _a = csv_import_generator(file)
        for row in _a:
            user_dict[row[0]] = row[1]
        return user_dict["skill_path"], user_dict["event_path"], user_dict["start_date"], user_dict["end_date"]
    pass


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
            entry_present = sum(len(_rows) for _rows in
                                sql_arg.execute('SELECT * FROM regex_look WHERE regex=? and outcome=?',
                                                (entries[0], entries[1])))
                                                # Are the entries-csv values in DB?
            if entry_present == 0:
                # Generator will result in 0 if entries-csv values are absent.
                sql_arg.execute('INSERT into regex_look VALUES (?,?)', (entries[0], entries[1]))
    return sql_arg


def csv_import_generator(file):
    with open(file) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        csv_reader = csv.reader(csvfile, dialect)
        for row in csv_reader:
            yield row


ud = UserData('my data.txt')
sk = SkillLog(ud.skill_path)
ev = EventLog(ud.event_path, ud.start_date)

con = sql.connect("regex.sqlite")
con.isolation_level = None

get_regex(con)