from datetime import datetime, timedelta
from collections import namedtuple
import csv
import sqlite3 as sql
import re


class LogFile:
    def __init__(self, log_path):
        self.log_path = log_path
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()
        self.datetime_whole = datetime
        self.line_list = list()
        self.start_date = ud.start_date
        self.log_position = self.get_log_position()
        self.Line = namedtuple('Line', ['time', 'line'])

    def __iter__(self):
        return

    def __next__(self, _count=50):
        with open(self.log_path, encoding='utf-8') as _fp:
            _fp.seek(self.log_position)
            for _line in iter(_fp.readline, ''):
                self.log_position = _fp.tell()
                _line = _line.strip()
                date1, time1 = time_stamp(_line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                self.line_list.append(self.Line(self.datetime_whole, _line[11:]))
                _count -= 1
                if _count <= 0:
                    break

    def get_log_position(self):
        _log_position = list()
        with open(self.log_path, encoding='utf-8') as _fp:
            for _line in iter(_fp.readline, ''):
                _log_position.insert(0, _fp.tell())
                _log_position = _log_position[:2]
                _line = _line.strip()
                date1, time1 = time_stamp(_line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                if self.date_part.year == 1:
                    continue
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.start_date <= self.datetime_whole:
                    return _log_position[1]

    def line_consumer(self, line_count=0):
        if len(self.line_list) < line_count:
            line_count = len(self.line_list)
        for _ in range(line_count):
            try:
                _line = self.line_list.pop(0)
            except IndexError:
                raise StopIteration
            yield _line

    pass


class UserData:
    def __init__(self, file):
        self.skill_path, self.event_path, self.start_date, self.end_date = fetch_my_data(file)
        if type(eval(self.start_date)) is datetime:
            self.start_date = eval(self.start_date)
        else:
            raise ValueError
        if type(eval(self.end_date)) is datetime:
            self.end_date = eval(self.end_date)
        else:
            raise ValueError

    pass


class Skill:
    def __init__(self):
        self.found_times = dict()

    def group_same_times(self, _line_cnt, _log_class):
        """

        :param _log_class: LogFile
        :param _line_cnt: int
        """
        for _line in _log_class.line_consumer(_line_cnt):
            if _line.time in self.found_times:
                self.found_times[_line.time].append(_line.line)
            else:
                self.found_times[_line.time] = [_line.line]
    pass


class Event:
    def __init__(self):
        self.line_matches = list()
        self.Line = namedtuple('Line', ['time', 'line', 'tool', 'target'])

    def line_matcher(self, _line_cnt, _log_class, _sql_con):
        """

        :param _line_cnt: int
        :param _log_class: LogFile
        :param _sql_con: sql.connect
        """
        for _line in _log_class.line_consumer(_line_cnt):
            with _sql_con:
                for _row in _sql_con.execute('SELECT * FROM REGEX_LOOK'):
                    result = re.search(_row[0], _line.line)
                    if result:
                        break
                if result:

                    self.line_matches.append(self.Line(time=_line.time, line=_line.line))
                    print(result.groups(), result.group(0), result.group(1), result.group(2))

    pass


def fetch_my_data(file):
    user_dict = {}
    _csv = csv_import_generator(file)
    for _row in _csv:
        user_dict[_row[0]] = _row[1]
    return user_dict["skill_path"], user_dict["event_path"], user_dict["start_date"], user_dict["end_date"]


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
sk_log = LogFile(ud.skill_path)
ev_log = LogFile(ud.event_path)
sk_data = Skill()
ev_data = Event()


con = sql.connect("regex.sqlite")
con.isolation_level = None

get_regex(con)

sk_log.__next__(50)
sk_data.group_same_times(50, sk_log)

ev_log.__next__(75)

ev_data.line_matcher(50, ev_log, con)
